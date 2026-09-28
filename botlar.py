import argparse
import json
import os
import random
import sys
import time

import grpc
from solana.rpc.api import Client
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

from jito_searcher_client.convert import versioned_tx_to_protobuf_packet
from jito_searcher_client.generated.auth_pb2 import (
    GenerateAuthChallengeRequest,
    GenerateAuthTokensRequest,
    Role,
)
from jito_searcher_client.generated.auth_pb2_grpc import AuthServiceStub
from jito_searcher_client.generated.bundle_pb2 import Bundle
from jito_searcher_client.generated.searcher_pb2 import (
    GetTipAccountsRequest,
    SendBundleRequest,
)
from jito_searcher_client.generated.searcher_pb2_grpc import SearcherServiceStub


def parse_args():
    parser = argparse.ArgumentParser(description="send jito bundles and track land rate")
    parser.add_argument("--keypair", required=True, help="path to the keypair json")
    parser.add_argument(
        "--block-engine", required=True, help="block engine host:port (grpc)"
    )
    parser.add_argument("--rpc", required=True, help="solana rpc url")
    parser.add_argument("--tip", type=int, required=True, help="tip in lamports")
    parser.add_argument("--bundles", type=int, default=1, help="how many bundles")
    parser.add_argument(
        "--stats-file", default="bundle_stats.json", help="where to keep the counters"
    )
    return parser.parse_args()


def load_keypair(path):
    with open(os.path.expanduser(path)) as f:
        data = json.load(f)
    return Keypair.from_bytes(bytes(data))


def get_auth_token(channel, kp):
    # ask the block engine for a challenge, sign it and exchange it for a token
    auth_client = AuthServiceStub(channel)
    challenge = auth_client.GenerateAuthChallenge(
        GenerateAuthChallengeRequest(role=Role.SEARCHER, pubkey=bytes(kp.pubkey()))
    ).challenge
    challenge_str = f"{str(kp.pubkey())}-{challenge}"
    signed_challenge = kp.sign_message(bytes(challenge_str, "utf8"))
    auth_tokens = auth_client.GenerateAuthTokens(
        GenerateAuthTokensRequest(
            challenge=challenge_str,
            client_pubkey=bytes(kp.pubkey()),
            signed_challenge=bytes(signed_challenge),
        )
    )
    return auth_tokens.access_token.value


def pick_tip_account(client, metadata):
    accounts = client.GetTipAccounts(GetTipAccountsRequest(), metadata=metadata).accounts
    if not accounts:
        raise RuntimeError("GetTipAccounts came back empty")
    return Pubkey.from_string(random.choice(accounts))


def build_bundle(rpc, kp, tip_account, tip_lamports):
    blockhash = rpc.get_latest_blockhash().value.blockhash

    # first tx is the actual transaction, second one pays the tip
    ixs = [
        transfer(
            TransferParams(from_pubkey=kp.pubkey(), to_pubkey=kp.pubkey(), lamports=2_000)
        )
    ]
    msg = MessageV0.try_compile(kp.pubkey(), ixs, [], blockhash)
    tx = VersionedTransaction(msg, [kp])

    tip_ixs = [
        transfer(
            TransferParams(
                from_pubkey=kp.pubkey(), to_pubkey=tip_account, lamports=tip_lamports
            )
        )
    ]
    tip_msg = MessageV0.try_compile(kp.pubkey(), tip_ixs, [], blockhash)
    tip_tx = VersionedTransaction(tip_msg, [kp])

    return [tx, tip_tx]


def wait_for_landed(rpc, sig, timeout=30):
    # poll the rpc until the signature shows up or we give up
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = rpc.get_signature_statuses([sig])
            status = resp.value[0]
            if status is not None and status.confirmation_status is not None:
                return True
        except Exception as e:
            print("status check failed:", e)
        time.sleep(2)
    return False


def load_stats(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"sent": 0, "landed": 0}


def save_stats(path, stats):
    with open(path, "w") as f:
        json.dump(stats, f, indent=2)


def main():
    args = parse_args()

    kp = load_keypair(args.keypair)
    rpc = Client(args.rpc)
    channel = grpc.secure_channel(args.block_engine, grpc.ssl_channel_credentials())

    try:
        token = get_auth_token(channel, kp)
    except Exception as e:
        print("auth failed:", e)
        sys.exit(1)

    metadata = [("authorization", f"Bearer {token}")]
    client = SearcherServiceStub(channel)

    try:
        tip_account = pick_tip_account(client, metadata)
    except Exception as e:
        print("could not get a tip account:", e)
        sys.exit(1)
    print("using tip account", tip_account)

    stats = load_stats(args.stats_file)
    for i in range(args.bundles):
        txs = build_bundle(rpc, kp, tip_account, args.tip)
        packets = [versioned_tx_to_protobuf_packet(tx) for tx in txs]
        try:
            uuid_response = client.SendBundle(
                SendBundleRequest(bundle=Bundle(header=None, packets=packets)),
                metadata=metadata,
            )
        except Exception as e:
            print("send bundle failed:", e)
            continue

        stats["sent"] += 1
        print(f"bundle {i + 1}/{args.bundles} sent, uuid={uuid_response.uuid}")

        # the whole bundle lands or nothing does, so checking the first tx is enough
        if wait_for_landed(rpc, txs[0].signatures[0]):
            stats["landed"] += 1
            print("  landed")
        else:
            print("  did not land")
        save_stats(args.stats_file, stats)

    rate = stats["landed"] / stats["sent"] * 100 if stats["sent"] else 0
    print(f"land rate: {stats['landed']}/{stats['sent']} ({rate:.1f}%)")


if __name__ == "__main__":
    main()

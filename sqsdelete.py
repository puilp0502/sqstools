#!/usr/bin/env python3
# sqsdelete.py: Delete messages from SQS
# Usage: ./sqsdelete.py <queue-url> < file-containing-receipt-handles.txt
import itertools
import sys
from typing import Iterable, List, Tuple

import boto3


def delete_message_batch(
    client,
    queue_url: str,
    receipt_handles: List[Tuple[str, int]],
):
    batch_delete_result = client.delete_message_batch(
        QueueUrl=queue_url,
        Entries=[
            {"Id": str(i), "ReceiptHandle": handle} for i, handle in receipt_handles
        ],
    )
    if "Failed" in batch_delete_result:
        for failed in batch_delete_result["Failed"]:
            print(failed)


def chunkify(src: Iterable, n: int):
    it = iter(src)
    while True:
        next_chunk = list(itertools.islice(it, n))
        if len(next_chunk) == 0:
            break
        yield next_chunk


def main():
    if len(sys.argv) < 1:
        print("Usage: ./sqsdelete.py <queue-url>")
        return

    sqs_client = boto3.client("sqs")
    queue_url = sys.argv[1]
    handles = enumerate(filter(lambda x: x.strip() != "", sys.stdin), start=1)
    for chunk in chunkify(handles, 10):
        delete_message_batch(sqs_client, sys.argv[1], chunk)


if __name__ == "__main__":
    main()

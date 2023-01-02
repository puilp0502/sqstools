#!/usr/bin/env python3
# sqspush.py: Push messages into SQS
# Usage: ./sqspush.py <queue-url> < input-file.jsonl
import itertools
import sys
from typing import Iterable, List, Tuple

import boto3
from tqdm import tqdm


def send_message_batch(client, queue_url: str, payloads: List[str]):
    send_result = client.send_message_batch(
        QueueUrl=queue_url,
        Entries=[
            {"Id": str(i), "MessageBody": payload} for i, payload in enumerate(payloads)
        ],
    )
    failed = send_result.get('Failed')
    if failed is None or not isinstance(failed, list):
        return
    if len(failed) == 0:
        return

    failed_ids = map(lambda x: x['Id'], failed)
    payloads = [payloads[int(i)] for i in failed_ids]
    for x in payloads:
        print(x)


def divide_chunk(inputs: List[str], maxsize: int):
    byte_sizes = [len(s.encode("utf-8")) for s in inputs]
    current_batch_size = 0
    current_batch = []
    for i, payload in enumerate(inputs):
        current_batch.append(payload)
        current_batch_size += byte_sizes[i]
        if current_batch_size + byte_sizes[i] > maxsize:
            yield current_batch
            current_batch = []
            current_batch_size = 0
    if len(current_batch) > 0:
        yield current_batch


def chunkify(src: Iterable[str], n: int, maxsize: int):
    it = iter(src)
    while True:
        next_chunk = list(itertools.islice(it, n))
        if len(next_chunk) == 0:
            break
        for split_chunk in divide_chunk(next_chunk, maxsize):
            yield split_chunk


def main():
    if len(sys.argv) < 2:
        print("Usage: ./sqspush.py <queue-url>")
        return

    sqs_client = boto3.client("sqs")
    queue_url = sys.argv[1]
    inputs = filter(lambda x: x.strip() != "", sys.stdin)
    for chunk in chunkify(tqdm(inputs), 10, 262144):
        send_message_batch(sqs_client, queue_url, chunk)


if __name__ == "__main__":
    main()

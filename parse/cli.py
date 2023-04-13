#!/usr/bin/env python3
import argparse
import json
import logging
import sys
from typing import Union
import json_lines

from parse import query

log = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description="CLI for the python json query language implementation"
)

parser.add_argument("--version", "-v", action="version", version= "0.1")

parser.add_argument("query", type=str, help="The query to run")

inputgroup = parser.add_mutually_exclusive_group()
inputgroup.add_argument("--data", "-d", type=str, help="The data to run the query on.")
inputgroup.add_argument(
    "--file", "-f", type=str, help="The file to read the data from. Defaults to stdin"
)

inputgroup.add_argument(
    "--file_jsonl", "-fjl", type=str, help="The json-lines file to read the data from."
)

parser.add_argument(
    "--output", "-o", type=str, help="The output file. Defaults to stdout"
)

parser.add_argument(
    "--pretty", "-p", action="store_true", help="Pretty print the output"
)


def main(supplied_args=None):
    if supplied_args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(supplied_args)
    raw_data: Union[str, bytes]

    if args.data:
        raw_data = args.data
    elif args.file:
        with open(args.file, 'rb') as f:
            raw_data = f.read()

    elif args.file_jsonl:
        out = []
        with open(args.file_jsonl, 'rb') as f:
            for item in json_lines.reader(f):
                out.append(query(args.query, item))
    else:
        raw_data = sys.stdin.buffer.read()

    if not args.file_jsonl:
        data = json.loads(raw_data)
        out = query(args.query, data)
    if args.output:
        # TODO: Allow alternate output encodings other than utf-8
        out_bytes = json.dumps(
            out,
            indent=2 if args.pretty else None,
            ensure_ascii=False
        ).encode("utf-8")
        with open(args.output, "wb") as f:
            f.write(out_bytes)
    else:
        print(json.dumps(out, indent=2 if args.pretty else None, ensure_ascii=False))


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""Builds redis_commands.json from the src/commands folder of a BSD-licensed Redis release (7.2.x).

Usage: generate_redis_commands.py <redis-source>/src/commands > redis_commands.json
"""
import json
import os
import re
import sys

GROUPS = {
    "bitmap": "Bitmap",
    "cluster": "Cluster Management",
    "connection": "Connection Management",
    "generic": "Keys",
    "geo": "Geospatial Indices",
    "hash": "Hash",
    "hyperloglog": "HyperLogLog",
    "list": "List",
    "pubsub": "Pub/Sub",
    "scripting": "Scripting and Functions",
    "sentinel": "Sentinel",
    "server": "Server Management",
    "set": "Set",
    "sorted_set": "Sorted Set",
    "stream": "Stream",
    "string": "String",
    "transactions": "Transactions",
}


def argument(spec):
    """One argument the way redis.io writes syntax, e.g. `[LIMIT offset count]`, `<NX | XX>`."""
    kind = spec["type"]
    if kind == "pure-token":
        return optional(spec, spec.get("token", spec["name"]).upper())
    if kind == "oneof":
        inner = " | ".join(argument(each) for each in spec["arguments"])
        if "token" in spec or not spec.get("optional"):
            inner = f"<{inner}>"
    elif kind == "block":
        inner = " ".join(argument(each) for each in spec["arguments"])
    else:
        inner = spec.get("display_text", spec["name"])
    token = spec.get("token", "").upper()
    text = f"{token} {inner}" if token else inner
    if spec.get("multiple"):
        repeated = text if spec.get("multiple_token") else inner
        text = f"{text} [{repeated} ...]"
    return optional(spec, text)


def optional(spec, text):
    return f"[{text}]" if spec.get("optional") else text


def plain(text):
    """Drops redis.io markdown code marks: `SET` and `!GET` (a mark that suppresses a link)."""
    return re.sub(r"`!?([^`]*)`", r"\1", text)


def main(folder):
    commands = []
    for file in sorted(os.listdir(folder)):
        if not file.endswith(".json"):
            continue
        with open(os.path.join(folder, file)) as handle:
            ((name, spec),) = json.load(handle).items()
        if "container" in spec:
            name = f"{spec['container']} {name}"
        arguments = " ".join(argument(inner) for inner in spec.get("arguments", []))
        entry = {
            "name": name,
            "summary": plain(spec.get("summary", "")),
            "group": GROUPS.get(spec.get("group", ""), spec.get("group", "")),
            "since": spec.get("since", ""),
            "complexity": plain(spec.get("complexity", "")),
            "syntax": f"{name} {arguments}".strip(),
        }
        if "deprecated_since" in spec:
            entry["deprecated_since"] = spec["deprecated_since"]
            entry["replaced_by"] = plain(spec.get("replaced_by", ""))
        commands.append(entry)
    commands.sort(key=lambda entry: entry["name"])
    json.dump(commands, sys.stdout, indent=0, ensure_ascii=False)
    sys.stdout.write("\n")


main(sys.argv[1])

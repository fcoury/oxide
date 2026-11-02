#!/usr/bin/env python3
"""Call one Oxide MCP tool over stdio.

The server binary comes from OXIDE_BIN (default: oxide). DATABASE_URL, when
set, is passed as --postgres-url. Pass --allow-writes before the tool name to
start the server with writes enabled.

    python3 scripts/mcp-stdio.py list_collections '{"database":"bookstore"}'
    python3 scripts/mcp-stdio.py --allow-writes insert '{"database":"bookstore","collection":"books","documents":[{"title":"Example"}]}'
"""

import json
import os
import subprocess
import sys


def main() -> int:
    args = sys.argv[1:]
    allow_writes = False
    if args and args[0] == "--allow-writes":
        allow_writes = True
        args = args[1:]
    if len(args) < 1 or args[0] in {"-h", "--help"}:
        print(__doc__.strip())
        return 2
    tool = args[0]
    arguments = json.loads(args[1]) if len(args) > 1 else {}
    binary = os.environ.get("OXIDE_BIN", "oxide")
    command = [binary, "mcp"]
    url = os.environ.get("DATABASE_URL")
    if url:
        command.extend(["--postgres-url", url])
    if allow_writes:
        command.append("--allow-writes")
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "error")
    proc = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert proc.stdin is not None and proc.stdout is not None

    def send(message: dict) -> None:
        proc.stdin.write(json.dumps(message) + "\n")
        proc.stdin.flush()

    def recv() -> dict:
        line = proc.stdout.readline()
        if not line:
            proc.kill()
            raise SystemExit("oxide mcp closed stdout before answering")
        return json.loads(line)

    try:
        send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "oxide-mcp-stdio", "version": "0.2.0"},
                },
            }
        )
        ready = recv()
        if "error" in ready:
            print(json.dumps(ready["error"], indent=2))
            return 1
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        send(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": tool, "arguments": arguments},
            }
        )
        response = recv()
    finally:
        proc.stdin.close()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    if "error" in response:
        print(json.dumps(response["error"], indent=2))
        return 1
    result = response.get("result", {})
    text = "".join(
        block.get("text", "")
        for block in result.get("content", [])
        if block.get("type") == "text"
    )
    print(text)
    return 1 if result.get("isError") else 0


if __name__ == "__main__":
    raise SystemExit(main())

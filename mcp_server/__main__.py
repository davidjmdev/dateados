"""Entrypoint CLI para el servidor MCP de Dateados.

Uso:
    python -m mcp_server                          # stdio (default)
    python -m mcp_server --transport sse           # SSE en puerto 8080
    python -m mcp_server --transport sse --port 9000  # SSE en puerto custom
"""

import argparse
import sys

from mcp_server.server import create_server


def main():
    parser = argparse.ArgumentParser(
        description="Dateados NBA Stats MCP Server"
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="Protocolo de transporte (default: stdio)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Puerto para SSE transport (default: 8080)",
    )

    args = parser.parse_args()

    mcp = create_server()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
    elif args.transport == "sse":
        mcp.run(transport="sse", port=args.port)


if __name__ == "__main__":
    main()

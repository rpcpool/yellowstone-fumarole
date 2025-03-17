import click
import importlib.metadata
import sys
import toml
import grpc
import os
from pathlib import Path

from fume.cli.stream_commands import stream
from fume.cli.cg_commands import create_cg, list_cg, delete_cg, delete_all_cg, get_cg
from fume.grpc import FumaroleClient, grpc_channel


def parse_key_value(ctx, param, value):
    # Parse the input as key:value pairs into a dictionary
    pairs = []
    for item in value:
        key, val = item.split(":", 1)  # split only on the first colon
        pairs.append((key, val))
    return pairs


def default_fume_config_path():
    default = Path.home() / ".config" / "fume" / "config.toml"
    return os.environ.get("FUME_CONFIG", default)


@click.group()
@click.option(
    "--config",
    help="Path to configuration file",
    type=click.Path(exists=False, readable=True),
    show_default=True,
    default=default_fume_config_path,
)
@click.option(
    "--endpoints", help="Comma separated list of endpoints to fumarole", type=str
)
@click.option(
    "--x-token", help="Access token for authentication", type=str, required=False
)
@click.option(
    "-X",
    "--x-header",
    help="Metadata key value pairs",
    type=str,
    multiple=True,
    required=False,
    callback=parse_key_value,
)
@click.pass_context
def cli(ctx, config, endpoints, x_token, x_header):
    if ctx.invoked_subcommand == "version":
        return
    try:
        with open(config) as f:
            config = toml.load(f)
            ctx.obj = {"conn": config["fumarole"]}
            metadata = [
                (k, v)
                for k, v in config.get("fumarole", {}).items()
                if k.startswith("x-") and k != "x-token"
            ]
            ctx.obj["conn"]["grpc-metadata"] = metadata
    except FileNotFoundError:
        ctx.obj = {"conn": {"grpc-metadata": []}}
        print(f"Warning: Configuration file not found {config}", file=sys.stderr)

    conn = ctx.obj.get("conn", {})
    if endpoints:
        conn["endpoints"] = endpoints.split(",")

    if x_token:
        conn["x-token"] = x_token

    for x_header_key, x_header_value in x_header:
        conn["grpc-metadata"].append((x_header_key, x_header_value))

    if conn.get("compression") == "gzip":
        conn["compression"] = grpc.Compression.Gzip
    elif conn.get("compression") == "none":
        conn["compression"] = grpc.Compression.NoCompression
    elif conn.get("compression") is None:
        conn["compression"] = grpc.Compression.NoCompression
    else:
        click.echo(
            'Error: Invalid compression type, supports "gzip" or "none".', err=True
        )
        sys.exit(1)

    if not conn.get("x-token"):
        click.echo("Warning: No access token provided", err=True)

    if not conn.get("endpoints"):
        click.echo("Error: No endpoints provided", err=True)
        sys.exit(1)

    return


@cli.command()
def version():
    print(f"{importlib.metadata.version('yellowstone-fume')}")


@cli.command(help="Test configuration file")
@click.option(
    "--connect/--no-connect",
    help="Connect to fumarole endpoints in configuration file",
    default=True,
)
@click.pass_context
def test_config(ctx, connect):
    conn = ctx.obj.get("conn")
    endpoints = conn.get("endpoints")
    x_token = conn.get("x-token")
    metdata = conn.get("grpc-metadata")

    if not connect:
        return

    if not endpoints and connect:
        click.echo("Error: No endpoints provided -- can't ", err=True)
        sys.exit(1)

    for e in endpoints:
        with grpc_channel(e, x_token) as c:
            fc = FumaroleClient(c, metadata=metdata)
            fc.list_available_commitments()
            click.echo(f"Sucessfully connected to {e}")

    click.echo("Configuration file is valid")


cli.add_command(stream)
cli.add_command(create_cg)
cli.add_command(list_cg)
cli.add_command(delete_cg)
cli.add_command(delete_all_cg)
cli.add_command(get_cg)


def main():
    cli()


if __name__ == "__main__":
    main()

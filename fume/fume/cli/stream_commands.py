import multiprocessing
import random
import string
import click
from fume.grpc import (
    FumaroleClient,
    grpc_channel,
    subscribe_update_to_dict,
    subscribe_update_to_summarize,
)


def generate_random_cg_name():
    """Generate a random consumer group name."""
    random_suffix = "".join(random.choices(string.ascii_lowercase, k=6))
    return f"fume-{random_suffix}"


@click.command()
@click.option(
    "--cg-name",
    help="""Consumer group name to subscribe to, if none provided a random name will be generated following the pattern 'fume-<random-6-character>'.""",
    type=str,
    default=generate_random_cg_name,
)
@click.option(
    "-p",
    "--parallel",
    help="Number of parallel consumers, the number cannot be greater than the size of group",
    type=int,
    default=1,
)
@click.option(
    "--tx-account",
    help="""
    Filter transaction whose account keys include the provided value in base58 format.
    You can provide multiple values by using the option multiple times.
    """,
    type=str,
    multiple=True,
    required=False,
    show_default=True,
)
@click.option(
    "--account",
    help="""
    Filter Account update whose account keys include the provided value in base58 format.
    """,
    type=str,
    multiple=True,
    required=False,
    show_default=True,
)
@click.option(
    "--owner",
    help="""
    Filter Account update whose account owner match the provided value in base58 format.
    You can provide multiple values by using the option multiple times.
    """,
    type=str,
    multiple=True,
    required=False,
    show_default=True,
)
@click.option(
    "-o",
    "--output-format",
    help="Output format",
    type=click.Choice(["json", "summ"]),
    default="summ",
)
@click.pass_context
def stream(ctx, cg_name, parallel, tx_account, account, owner, output_format):
    """Stream JSON data from Fumarole."""
    conn = ctx.obj["conn"]
    endpoints = conn["endpoints"]
    x_token = conn.get("x-token")
    metadata = conn.get("grpc-metadata")

    subscribe_request = {}

    if tx_account:
        subscribe_request["transactions"] = {}
        subscribe_request["transactions"]["account_keys"] = list(tx_account)
    if account or owner:
        subscribe_request["accounts"] = {}
        if account:
            subscribe_request["accounts"]["account"] = list(account)
        if owner:
            subscribe_request["accounts"]["owner"] = list(owner)

    def fumarole_stream_proc(shared_q, cg_name, member_idx, endpoint, x_token):
        with grpc_channel(endpoint, x_token) as channel:
            fc = FumaroleClient(channel, metadata=metadata)
            for event in fc.subscribe(
                cg_name, member_idx, mapper=output_format, **subscribe_request
            ):
                shared_q.put(event)

    def stream_loop(mpq, fumarole_ps):
        while all(p.is_alive() for p in fumarole_ps):
            data = mpq.get()
            click.echo(data)

    mpq = multiprocessing.Queue()
    fumarole_ps = []
    for i in range(parallel):
        j = i % len(endpoints)
        endpoint = endpoints[j]
        print(f"Spawned fumarole connection: {i}...")
        fumarole = multiprocessing.Process(
            target=fumarole_stream_proc, args=(mpq, cg_name, i, endpoint, x_token)
        )
        fumarole.start()
        click.echo(
            f"Started fumarole connection: {i} with pid={fumarole.pid}!", err=True
        )
        fumarole_ps.append(fumarole)

    try:
        stream_loop(mpq, fumarole_ps)
        click.echo("All connections closed", err=True)
    except KeyboardInterrupt:
        click.echo("Ctrl+C detected, cleaning up...", err=True)
    except EOFError:
        click.echo("Ctrl+D detected, cleaning up...", err=True)
    finally:
        click.echo("Closing all connections...", err=True)
        for p in fumarole_ps:
            p.terminate()
        for p in fumarole_ps:
            p.join()

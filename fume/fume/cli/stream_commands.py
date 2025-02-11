from dataclasses import dataclass
import multiprocessing
import multiprocessing.queues
import queue
import random
import signal
import string
from typing import Optional
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


@dataclass
class StopFumaroleStream:
    pass


@dataclass
class FumaroleStreamData:
    data: any


@dataclass
class FumaroleStreamEnd:
    pid: int


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

    def fumarole_stream_proc(
        cnc_rx: multiprocessing.Queue,  # command-and-control queue
        data_tx: multiprocessing.Queue,
        cg_name: str,
        member_idx: int,
        endpoint: str,
        x_token: Optional[str],
    ):
        signal.signal(signal.SIGINT, signal.SIG_IGN)  # Ignore Ctrl+C in subprocess
        with grpc_channel(endpoint, x_token) as channel:
            fc = FumaroleClient(channel, metadata=metadata)

            my_pid = multiprocessing.current_process().pid
            subscribe_iter = fc.subscribe(
                cg_name, member_idx, mapper=output_format, **subscribe_request
            )
            for event in subscribe_iter:
                data_tx.put(FumaroleStreamData(data=event))
                try:
                    command = cnc_rx.get_nowait()
                    match command:
                        case StopFumaroleStream():
                            break
                except queue.Empty:
                    pass

            channel.close()
            # Flush any remaining data
            for event in subscribe_iter:
                data_tx.put(event)

            data_tx.put(FumaroleStreamEnd(pid=my_pid))

    data_tx = multiprocessing.Queue()
    data_rx = data_tx
    fumarole_ps: dict[multiprocessing.Process, int] = dict()
    fumarole_cnc_tx_vec = []
    fumarole_stream_id_vec = set()
    for i in range(parallel):
        j = i % len(endpoints)
        endpoint = endpoints[j]
        print(f"Spawned fumarole connection: {i}...")

        cnc_tx = multiprocessing.Queue()
        cnc_rx = cnc_tx
        fumarole: multiprocessing.Process = multiprocessing.Process(
            target=fumarole_stream_proc,
            args=(cnc_rx, data_tx, cg_name, i, endpoint, x_token),
        )

        fumarole.start()
        click.echo(
            f"Started fumarole connection: {i} with pid={fumarole.pid}!", err=True
        )
        fumarole_ps[fumarole.pid] = fumarole
        fumarole_cnc_tx_vec.append(cnc_tx)
        fumarole_stream_id_vec.add(fumarole.pid)

    while True:
        try:
            if not all(p.is_alive() for p in fumarole_ps.values()):
                break
            match data_rx.get():
                case FumaroleStreamData(data):
                    click.echo(data)
                case FumaroleStreamEnd(pid):
                    fumarole_stream_id_vec.remove(pid)
                    click.echo(f"Connection {pid} ended!", err=True)
                    break
            click.echo(data)
        except KeyboardInterrupt:
            break
        except EOFError:
            break

    for cnc_tx in fumarole_cnc_tx_vec:
        cnc_tx.put(StopFumaroleStream())

    # Drain any leftover data in case some is left in the queue.
    while fumarole_stream_id_vec or any(p.is_alive() for p in fumarole_ps.values()):
        try:
            match data_rx.get(timeout=1):
                case FumaroleStreamData(data):
                    click.echo(data)
                case FumaroleStreamEnd(pid):
                    fumarole_stream_id_vec.remove(pid)
                    fumarole_proc = fumarole_ps.pop(pid)
                    fumarole_proc.terminate()
                    fumarole_proc.join()
        except queue.Empty:
            pass

    for _, fumarole_proc in fumarole_ps.items():
        fumarole_proc.terminate()
        fumarole_proc.join()

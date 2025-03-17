import random
import string
import click
from fume.grpc import FumaroleClient, grpc_channel
from tabulate import tabulate
import yellowstone_api.fumarole_pb2 as fumarole_p2b
from yellowstone_api.fumarole_pb2 import ConsumerGroupInfo


def generate_random_cg_name():
    """Generate a random consumer group name."""
    random_suffix = "".join(random.choices(string.ascii_lowercase, k=6))
    return f"fume-{random_suffix}"


def cg_info_into_text_row(cg: ConsumerGroupInfo) -> list[str]:
    if cg.commitment_level == fumarole_p2b.PROCESSED:
        cl = "processed"
    elif cg.commitment_level == fumarole_p2b.CONFIRMED:
        cl = "confirmed"
    elif cg.commitment_level == fumarole_p2b.FINALIZED:
        cl = "finalized"
    else:
        cl = "???"

    if cg.event_subscription_policy == fumarole_p2b.ACCOUNT_UPDATE_ONLY:
        sub_policy = "account"
    elif cg.event_subscription_policy == fumarole_p2b.TRANSACTION_ONLY:
        sub_policy = "tx"
    elif cg.event_subscription_policy == fumarole_p2b.BOTH:
        sub_policy = "account|tx"
    else:
        sub_policy = "???"

    row = [cg.id, cg.consumer_group_label, cg.member_count, cl, sub_policy, cg.is_stale]
    return row


@click.command()
@click.option(
    "--name",
    help="""Consumer group name to subscribe to, if none provided a random name will be generated following the pattern 'fume-<random-6-character>'.""",
    type=str,
    default=generate_random_cg_name,
)
@click.option(
    "--size",
    help="Size of the consumer group",
    type=int,
    default=1,
)
@click.option(
    "--commitment",
    help="Commitment level",
    type=click.Choice(["processed", "confirmed", "finalized"]),
    default="confirmed",
    show_default=True,
    required=False,
)
@click.option(
    "--include",
    help="Include option",
    type=click.Choice(["all", "account", "tx"]),
    default="all",
    show_default=True,
    required=False,
)
@click.option(
    "--seek",
    help="Seek option",
    type=click.Choice(["earliest", "latest", "slot"]),
    default="latest",
    show_default=True,
    required=False,
)
@click.pass_context
def create_cg(
    ctx,
    name,
    size,
    commitment,
    include,
    seek,
):
    """Creates a consumer group"""
    conn = ctx.obj["conn"]
    endpoints = conn["endpoints"]
    x_token = conn.get("x-token")
    metadata = conn.get("grpc-metadata")
    compression = conn.get("compression")

    with grpc_channel(endpoints[0], x_token, compression=compression) as c:
        fc = FumaroleClient(c, metadata=metadata)

        name = fc.create_consumer_group(
            name=name,
            size=size,
            include=include,
            initial_seek=seek,
            commitment=commitment,
        )

        click.echo(f"Consumer group created: {name}")


@click.command()
@click.pass_context
def list_cg(
    ctx,
):
    """List active consumer groups"""
    conn = ctx.obj["conn"]
    endpoints = conn["endpoints"]
    x_token = conn.get("x-token")
    metadata = conn.get("grpc-metadata")
    compression = conn.get("compression")

    with grpc_channel(endpoints[0], x_token, compression=compression) as c:
        fc = FumaroleClient(c, metadata=metadata)

        cs = fc.list_consumer_groups()

        data = [["Id", "Name", "Size", "Commitment", "Subscriptions Policy", "Stale?"]]

        if not cs:
            click.echo("You have no consumer groups")
        else:

            for cg in cs:
                row = cg_info_into_text_row(cg)
                data.append(row)

            table = tabulate(data, headers="firstrow", tablefmt="grid")
            click.echo(table)


@click.command()
@click.option(
    "--name",
    help="""Consumer group name to subscribe to, if none provided a random name will be generated following the pattern 'fume-<random-6-character>'.""",
    type=str,
    default=generate_random_cg_name,
)
@click.pass_context
def delete_cg(ctx, name):
    """Delete a consumer group"""
    conn = ctx.obj["conn"]
    endpoints = conn["endpoints"]
    x_token = conn.get("x-token")
    metadata = conn.get("grpc-metadata")
    compression = conn.get("compression")

    with grpc_channel(endpoints[0], x_token, compression=compression) as c:
        fc = FumaroleClient(c, metadata=metadata)
        if click.confirm(f"Are you sure you want to delete consumer group {name}?"):
            is_deleted = fc.delete_consumer_group(name=name)
            if is_deleted:
                click.echo(f"Consumer group {name} deleted!")
            else:
                click.echo(f"Consumer group {name} not found!")

        click.echo("Done")


@click.command()
@click.pass_context
def delete_all_cg(ctx):
    """Deletes all consumer groups for current subscription"""
    conn = ctx.obj["conn"]
    endpoints = conn["endpoints"]
    x_token = conn.get("x-token")
    metadata = conn.get("grpc-metadata")
    compression = conn.get("compression")

    with grpc_channel(endpoints[0], x_token, compression=compression) as c:
        fc = FumaroleClient(c, metadata=metadata)

        cs = fc.list_consumer_groups()

        if not cs:
            click.echo("You have no consumer groups to delete!")
            return

        for cg in cs:
            click.echo(f"{cg.consumer_group_label}")

        if click.confirm(
            f"This operation will delete {len(cs)} consumer groups. Are you sure you want to proceed?"
        ):
            for cg in cs:
                fc.delete_consumer_group(name=cg.consumer_group_label)
                click.echo(f"Consumer group {cg.consumer_group_label} deleted!")


@click.command()
@click.option(
    "--name", help="""Get Consumer group info by name""", type=str, required=True
)
@click.pass_context
def get_cg(ctx, name):
    conn = ctx.obj["conn"]
    endpoints = conn["endpoints"]
    x_token = conn.get("x-token")
    metadata = conn.get("grpc-metadata")
    compression = conn.get("compression")

    with grpc_channel(endpoints[0], x_token, compression=compression) as c:
        fc = FumaroleClient(c, metadata=metadata)

        cg = fc.get_cg_info(name)
        if cg:

            data = [
                ["Id", "Name", "Size", "Commitment", "Subscriptions Policy", "Stale?"]
            ]
            row = cg_info_into_text_row(cg)
            data.append(row)
            table = tabulate(data, headers="firstrow", tablefmt="grid")
            click.echo(table)
        else:
            click.echo(f"Consumer group {name} not found", err=True)

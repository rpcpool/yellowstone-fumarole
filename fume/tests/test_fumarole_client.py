import os
import random
import string
import toml
import pytest
from fume.grpc import FumaroleClient, grpc_channel

@pytest.fixture
def test_config():
    test_config_path = os.environ.get('FUME_TEST_CONFIG')
    if test_config_path:
        with open(test_config_path) as f:
            config = toml.load(f)
            return config['fumarole']
    else:
        return {
            'endpoints': 'localhost:9000',
            'x-token': None,
            'x-subscription-id': '11111111-11111111-11111111-11111111'
        }


@pytest.fixture
def fumarole_client(test_config):

    metadata = [ (k, v) for k, v in test_config.items() 
                 if k.startswith('x-') and k != "x-token" ]

    with grpc_channel(test_config['endpoints'][0], test_config.get('x-token')) as channel:
        yield FumaroleClient(channel, metadata=metadata)


def random_str(len, prefix=None):
    suffix = ''.join(random.choices(string.ascii_letters, k=len))
    if prefix:
        return f"{prefix}-{suffix}"
    return suffix


def test_create_consumer_group(fumarole_client: FumaroleClient):
    cg_name = random_str(6, prefix='fume-test')
    commitment = 'confirmed'

    cg = fumarole_client.create_consumer_group(
        name=cg_name,
        commitment=commitment,
    )

    cg_info = fumarole_client.get_cg_info(cg_name)
    cg_list = fumarole_client.list_consumer_groups()
    assert cg_info
    assert cg_info.consumer_group_label == cg_name
    assert cg == cg_name
    assert cg_name in [cg.consumer_group_label for cg in cg_list]


def test_delete_consumer(fumarole_client: FumaroleClient):
    cg_name = random_str(6, prefix='fume-test')
    commitment = 'confirmed'

    cg = fumarole_client.create_consumer_group(
        name=cg_name,
        commitment=commitment,
    )

    cg_info = fumarole_client.get_cg_info(cg_name)
    fumarole_client.delete_consumer_group(cg_name)
    cg_info = fumarole_client.get_cg_info(cg_name)

    cg_list = fumarole_client.list_consumer_groups()

    assert not cg_info
    assert cg_name not in [cg.consumer_group_label for cg in cg_list]
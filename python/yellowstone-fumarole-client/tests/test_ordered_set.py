from yellowstone_fumarole_client.utils.collections import OrderedSet
import pytest


def test_ordered_set():
    os = OrderedSet()
    os.add(1)
    os.add(2)
    os.add(3)
    assert list(os) == [1, 2, 3]
    assert 2 in os
    assert len(os) == 3
    val = os.popfirst()
    assert val == 1
    assert list(os) == [2, 3]
    assert len(os) == 2


def test_pop_until_empty():
    os = OrderedSet()
    os.add(1)
    os.add(2)
    os.add(3)
    assert len(os) == 3
    os.popfirst()
    os.popfirst()
    os.popfirst()
    assert len(os) == 0
    with pytest.raises(KeyError):
        os.popfirst()


def test_it_should_rearrange_order_on_reinsert():
    os = OrderedSet()
    os.add(1)
    os.add(2)
    os.add(3)
    assert list(os) == [1, 2, 3]
    os.add(2)  # Re-insert 2
    assert list(os) == [1, 3, 2]  # 2 should now be at the end

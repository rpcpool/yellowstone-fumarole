from collections import OrderedDict


class OrderedSet:
    """
    A simple ordered set implementation that maintains the order of elements.
    """

    def __init__(self, iterable=None):
        self._data = []
        self.inner: OrderedDict = OrderedDict()
        if iterable is not None:
            for item in iterable:
                self.inner[item] = None

    def add(self, item):
        self.inner[item] = None
        # Make sure the item is at the end to maintain order
        self.inner.move_to_end(item, last=True)

    def popfirst(self):
        try:
            return self.inner.popitem(last=False)[0]
        except KeyError as e:
            raise KeyError(f"{OrderedDict.__name__} is empty") from e

    def __contains__(self, item):
        return item in self.inner

    def __iter__(self):
        return iter(self.inner.keys())

    def __len__(self):
        return len(self.inner)

    def __repr__(self):
        return f"OrderedSet({list(self._data)})"

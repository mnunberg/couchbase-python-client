from couchbase._libcouchbase import \
    LCB_SDCMD_REPLACE, LCB_SDCMD_DICT_ADD, LCB_SDCMD_DICT_UPSERT, \
    LCB_SDCMD_ARRAY_ADD_FIRST, LCB_SDCMD_ARRAY_ADD_LAST, \
    LCB_SDCMD_ARRAY_ADD_UNIQUE, LCB_SDCMD_EXISTS, LCB_SDCMD_GET, \
    LCB_SDCMD_COUNTER, LCB_SDCMD_REMOVE, LCB_SDCMD_ARRAY_INSERT

_Specmap = {}
for k, v in globals().items():
    if not k.startswith('LCB_SDCMD_'):
        continue
    k = k.replace('LCB_SDCMD_', '')
    _Specmap[v] = k


class Spec(tuple):
    def __new__(cls, *args, **kwargs):
        return super(Spec, cls).__new__(cls, tuple(args))

    def __repr__(self):
        details = []
        details.append(_Specmap.get(self[0]))
        details.extend([repr(x) for x in self[1:]])
        return '{0}<{1}>'.format(self.__class__.__name__,
                                 ', '.join(details))


def _gen_2spec(op, path):
    return Spec(op, path)


def _gen_4spec(op, path, value, create=False):
    return Spec(op, path, value, int(create))


class MultiValue(tuple):
    """
    This class is used to pass multiple values to :meth:`push_first` and
    :meth:`push_last`. Without this container object, the library will assume
    """
    def __new__(cls, *args, **kwargs):
        return super(MultiValue, cls).__new__(cls, tuple(args))

    def __repr__(self):
        return 'MultiValue({0})'.format(tuple.__repr__(self))


# The following functions return either 2-tuples or 4-tuples for operations
# which are converted into mutation or lookup specifications

def get(path):
    """
    Retrieve the value from the given path. The value is returned in the result
    :param path: The path to retrieve
    """
    return _gen_2spec(LCB_SDCMD_GET, path)


def exists(path):
    """
    Check if a given path exists. This is the same as :meth:`get()`,
    but the result will not contain the value
    :param path: The path to check
    """
    return _gen_2spec(LCB_SDCMD_EXISTS, path)


def replace(path, value):
    """
    Replace an existing path. This works on any valid path if the path already
    exists.
    :param path: The path to replace
    :param value: The new value
    """
    return _gen_4spec(LCB_SDCMD_REPLACE, path, value, False)


def insert(path, value, create_parents=False):
    """
    Create a new path in the document. The final path element points to a
    dictionary key that should be created.

    :param path: The path to create
    :param value: Value for the path
    :param create_parents: Whether intermediate parents should be created
    """
    return _gen_4spec(LCB_SDCMD_DICT_ADD, path, value, create_parents)


def upsert(path, value, create_parents=False):
    """
    Create or replace a dictionary path
    :param path: The path to modify
    :param value: The new value for the path
    :param create_parents: Whether intermediate parents should be created
    """
    return _gen_4spec(LCB_SDCMD_DICT_UPSERT, path, value, create_parents)


def push_last(path, *values, **kwargs):
    """
    Add new values to the end of an array
    :param path: Path to the array. The path should contain the _array itself_
    and not an element _within_ the array
    :param values: one or more values to append
    :param create_parents: Create the array if it does not exist
    """
    return _gen_4spec(LCB_SDCMD_ARRAY_ADD_LAST, path,
                      MultiValue(*values), kwargs.get('create_parents', False))


def push_first(path, *values, **kwargs):
    """
    Add new values to the beginning of an array
    :param path: Path to the array. The path should contain the _array itself_
    and not an element _within_ the array
    :param values: one or more values to append
    :param create_parents: Create the array if it does not exist
    """
    return _gen_4spec(LCB_SDCMD_ARRAY_ADD_FIRST, path,
                      MultiValue(*values), kwargs.get('create_parents', False))


def push_at(path, *values):
    """
    Insert items at a given position within an array
    :param path: The path indicating where the item should be placed. The path
        _should_ contain the desired position
    :param values: Values to insert
    """
    return _gen_4spec(LCB_SDCMD_ARRAY_INSERT, path,
                      MultiValue(*values), False)


def push_unique(path, value, create_parents=False):
    """
    Add a new value to an array if the value does not exist
    :param path: The path to the array
    :param value: Value to add to the array if it does not exist
    :param create_parents: Create the array if it does not exist
    """
    return _gen_4spec(LCB_SDCMD_ARRAY_ADD_UNIQUE, path, value, create_parents)


def counter(path, delta, create_parents=False):
    """
    Increment or decrement a counter in a document
    :param path: Path to the counter
    :param delta: Amount by which to modify the value (can be negative)
    :param create_parents: Create the counter (and apply the modification) if it
        does not exist
    """
    return _gen_4spec(LCB_SDCMD_COUNTER, path, delta, create_parents)


def remove(path):
    """
    Remove an existing path in the document
    :param path: The path to remove
    """
    return _gen_2spec(LCB_SDCMD_REMOVE, path)

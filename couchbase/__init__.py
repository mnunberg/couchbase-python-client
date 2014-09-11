#
# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from warnings import warn
from collections import namedtuple

from couchbase.user_constants import *
import couchbase._libcouchbase as _LCB

try:
    from couchbase._version import __version__

except ImportError:
    __version__ = "0.0.0-could-not-find-git"


def set_json_converters(encode, decode):
    """
    Modify the default JSON conversion functions. This affects all
    :class:`~couchbase.connection.Connection` instances.

    These functions will called instead of the default ones (``json.dumps``
    and ``json.loads``) to encode and decode JSON (when :const:`FMT_JSON` is
    used).

    :param callable encode: Callable to invoke when encoding an object to JSON.
        This should have the same prototype as ``json.dumps``, with the
        exception that it is only ever passed a single argument.

    :param callable decode: Callable to invoke when decoding an object to JSON.
        This should have the same prototype and behavior
        as ``json.loads`` with the exception that it is only ever
        passed a single argument.

    :return: A tuple of ``(old encoder, old decoder)``

    No exceptions are raised, and it is the responsibility of the caller to
    ensure that the provided functions operate correctly, otherwise exceptions
    may be thrown randomly when encoding and decoding values
    """
    ret = _LCB._modify_helpers(json_encode=encode, json_decode=decode)
    return (ret['json_encode'], ret['json_decode'])


def set_pickle_converters(encode, decode):
    """
    Modify the default Pickle conversion functions. This affects all
    :class:`~couchbase.connection.Connection` instances.

    These functions will be called instead of the default ones
    (``pickle.dumps`` and ``pickle.loads``) to encode and decode values to and
    from the Pickle format (when :const:`FMT_PICKLE` is used).

    :param callable encode: Callable to invoke when encoding an object to
        Pickle. This should have the same prototype as ``pickle.dumps`` with
        the exception that it is only ever called with a single argument

    :param callable decode: Callable to invoke when decoding a Pickle encoded
        object to a Python object. Should have the same prototype as
        ``pickle.loads`` with the exception that it is only ever passed a
        single argument

    :return: A tuple of ``(old encoder, old decoder)``

    No exceptions are raised and it is the responsibility of the caller to
    ensure that the provided functions operate correctly.
    """
    ret = _LCB._modify_helpers(pickle_encode=encode, pickle_decode=decode)
    return (ret['pickle_encode'], ret['pickle_decode'])

_DurabilityRequirements = namedtuple('_DurabilityRequirements', 'persist_to replicate_to')

class DurabilityRequirements(_DurabilityRequirements):
    """
    .. attribute persist_to::
        The minimum number of nodes which must contain
        this item on their disk before this function returns. Ensure that
        you do not specify too many nodes; otherwise this function will
        fail. Use the :attr:`server_nodes` to determine how many nodes
        exist in the cluster.

        The maximum number of nodes an item can reside on is currently
        fixed to 4 (i.e. the "master" node, and up to three "replica"
        nodes). This limitation is current as of Couchbase Server version
        2.1.0.

        If this parameter is set to a negative value, the maximum number
        of possible nodes the key can reside on will be used.

    .. attribute replicate_to::
        The minimum number of replicas which must
        contain this item in their memory for this method to succeed.
        As with ``persist_to``, you may specify a negative value in which
        case the requirement will be set to the maximum number possible.
    """
    MAX = (-1,-1)



class Couchbase(object):
    @classmethod
    def connect(self, *args, **kwargs):
        warn("Use the Bucket (in couchbase.bucket) constructor instead", DeprecationWarning)
        from couchbase.bucket import Bucket
        return Bucket(*args, **kwargs)


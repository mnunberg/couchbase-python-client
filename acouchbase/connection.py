import asyncio
from asyncio import Future, InvalidStateError

from acouchbase.asyncio_iops import IOPS
from couchbase.async.connection import Async


class ACouchbase(Async):
    def __init__(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        super(ACouchbase, self).__init__(IOPS(loop), *args, **kwargs)
        self._loop = loop

        cft = Future(loop=loop)
        def ftresult(err):
            if err:
                cft.set_exception(cfg)
            else:
                cft.set_result(True)

        self._cft = cft
        self._conncb = ftresult

    def _meth_factory(meth, name):
        def ret(self, *args, **kwargs):
            rv = meth(self, *args, **kwargs)
            ft = Future()
            def on_ok(res):
                ft.set_result(res)
                rv.callback = None
                rv.errback = None

            def on_err(res, excls, excval, exctb):
                rv.callback = None
                rv.errbac = None
                err = excls(excval)
                ft.set_exception(err)
            rv.callback = on_ok
            rv.errback = on_err
            return ft

        return ret

    locals().update(Async._gen_memd_wrappers(_meth_factory))

    def connect(self):
        if not self.connected:
            self._connect()
            return self._cft

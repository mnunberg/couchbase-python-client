import asyncio
from couchbase.iops.base import (
    IOEvent, TimerEvent, LCB_READ_EVENT, LCB_WRITE_EVENT,
    LCB_RW_EVENT, PYCBC_EVACTION_WATCH, PYCBC_EVACTION_UNWATCH
)

class AsyncioTimer(TimerEvent):
    def __init__(self):
        self._ashandle = None

    def cancel(self):
        if self._ashandle:
            self._ashandle.cancel()
            self._ashandle = None

    def schedule(self, loop, usec):
        sec = usec / 1000000
        self._ashandle = loop.call_later(sec, self.ready, 0)

class AsyncioEvent(IOEvent):
    def __init__(self, loop):
        self._loop = loop
        self.watched_for = 0

    def ready_r(self):
        self._loop.remove_reader(self.fd)
        self.watched_for &= ~LCB_READ_EVENT
        super(AsyncioEvent, self).ready_r()
    def ready_w(self):
        self._loop.remove_writer(self.fd)
        self.watched_for &= ~LCB_WRITE_EVENT
        super(AsyncioEvent, self).ready_w()

class IOPS(object):
    def __init__(self, evloop = None):
        if evloop is None:
            evloop = asyncio.get_event_loop()
        self.loop = evloop

    def update_event(self, event, action, flags):
        if action == PYCBC_EVACTION_UNWATCH:
            if event.watched_for & LCB_READ_EVENT:
                self.loop.remove_reader(event.fd)
            if event.watched_for & LCB_WRITE_EVENT:
                self.loop.remove_writer(event.fd)
            event.watched_for = 0
            return
        elif action == PYCBC_EVACTION_WATCH:
            if flags & LCB_READ_EVENT and not (event.watched_for & LCB_READ_EVENT):
                self.loop.add_reader(event.fd, event.ready_r)
            if flags & LCB_WRITE_EVENT and not (event.watched_for & LCB_WRITE_EVENT):
                self.loop.add_writer(event.fd, event.ready_w)
            event.watched_for = flags

    def update_timer(self, timer, action, usecs):
        timer.cancel()
        if action == PYCBC_EVACTION_UNWATCH:
            return
        timer.schedule(self.loop, usecs)

    def start_watching(self):
        pass
    def stop_watching(self):
        pass
    def timer_event_factory(self):
        return AsyncioTimer()
    def io_event_factory(self):
        return AsyncioEvent(self.loop)

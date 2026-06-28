"""serial_io.py — live + replay serial I/O for the Sail Race Tracker gateway.

This module owns the *transport*: it opens a serial port, reads raw bytes,
feeds them to :class:`srt_gateway.protocol.FrameParser`, decodes each frame
with :func:`srt_gateway.protocol.decode_payload`, and hands the result to a
caller-supplied ``on_frame`` callback. It never re-implements the wire codec —
all framing/CRC/decoding lives in ``protocol``.

Design goals:

* **No hard pyserial dependency at import time.** ``pyserial`` is imported
  lazily inside the methods that actually open a real port, so the module
  imports (and the test-suite runs) on a machine with no pyserial installed.
* **Dependency injection.** :class:`SerialReader` accepts an already-open
  serial-like object (``serial_obj=``) or an ``open_serial`` factory callable.
  Tests pass a :class:`FakeSerial`; production passes nothing and a real
  ``serial.Serial`` is opened by :meth:`SerialReader._open`.
* **Deterministic to test.** The read/parse cycle is a single
  :meth:`SerialReader.poll_once` call. :meth:`SerialReader.run` is just a loop
  over ``poll_once``; tests drive ``poll_once`` directly.
* **Survives the port vanishing.** The boards brown out / re-enumerate, so
  ``/dev/ttyACM*`` can disappear at any moment. A read error closes the port,
  flips ``connected`` false, backs off, re-detects + reopens, and resumes —
  the loop is never allowed to die.

Public API (see class docstrings for detail):

    find_base_port(configured=None, globs=(...)) -> str | None
    replay_bytelog(path, on_frame, chunk_size=4096, on_error=None) -> int
    class FakeSerial         # minimal pyserial stand-in for tests
    class SerialReader       # the live reader / replay driver
"""

import glob as _glob
import logging
import threading
import time

from . import protocol

__all__ = [
    "DEFAULT_PORT",
    "DEFAULT_BAUD",
    "PORT_GLOBS",
    "SerialError",
    "NotConnectedError",
    "find_base_port",
    "replay_bytelog",
    "FakeSerial",
    "SerialReader",
]

log = logging.getLogger("srt_gateway.serial_io")

DEFAULT_PORT = "/dev/ttyACM1"
DEFAULT_BAUD = 115200
# Search order for auto-detect: ACM (USB-CDC, the ESP32) first, then USB-serial.
PORT_GLOBS = ("/dev/ttyACM*", "/dev/ttyUSB*")


class SerialError(Exception):
    """Generic transport error raised by serial_io (not protocol)."""


class NotConnectedError(SerialError):
    """Raised by :meth:`SerialReader.send` when the port is not connected."""


def _serial_exception_types():
    """Return the exception classes that mean "the link broke".

    Imported lazily so the module works with no pyserial installed. We always
    include :class:`OSError` (file descriptor gone, ENODEV on unplug) and
    :class:`SerialError`; if pyserial is present we add its
    ``serial.SerialException``.
    """
    types = (OSError, SerialError)
    try:  # pragma: no cover - trivial import shim
        import serial  # type: ignore
        types = types + (serial.SerialException,)
    except Exception:
        pass
    return types


def find_base_port(configured=None, globs=PORT_GLOBS, _globber=None):
    """Return the serial device path for the base node.

    If ``configured`` is given (e.g. from config / CLI) it wins and is returned
    as-is — auto-detect is skipped. Otherwise each pattern in ``globs`` is
    expanded (ACM before USB) and the first matching device is returned.
    Returns ``None`` when nothing is found.

    ``_globber`` is an injection seam for tests (defaults to :func:`glob.glob`).
    """
    if configured:
        return configured
    globber = _globber or _glob.glob
    for pattern in globs:
        matches = sorted(globber(pattern))
        if matches:
            return matches[0]
    return None


def replay_bytelog(path, on_frame, chunk_size=4096, on_error=None):
    """Replay a captured raw byte-log through the *exact* live pipeline.

    Reads ``path`` (a binary capture of everything that came off the wire),
    feeds it to a fresh :class:`~srt_gateway.protocol.FrameParser` in
    ``chunk_size`` slices, decodes every recovered frame and calls
    ``on_frame(frame_type, payload, decoded)`` — identical to live serial, so
    the whole gateway can run with no hardware.

    A decoder that raises is caught (logged / forwarded to ``on_error``) and
    does not abort the replay. Returns the number of frames delivered.
    """
    parser = protocol.FrameParser()
    delivered = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            for ftype, payload in parser.push(chunk):
                if _dispatch(on_frame, ftype, payload, on_error):
                    delivered += 1
    return delivered


def _dispatch(on_frame, ftype, payload, on_error=None):
    """Decode one frame and invoke ``on_frame``; never raise.

    Returns True if the frame was delivered, False if decoding/dispatch raised
    (in which case the error is logged and optionally forwarded to ``on_error``).
    A buggy decoder or a buggy callback must not be able to kill the read loop.
    """
    try:
        decoded = protocol.decode_payload(ftype, payload)
    except Exception as exc:  # decoder bug — log and skip this frame
        name = protocol.TYPE_NAMES.get(ftype, "0x%02x" % ftype)
        log.exception("decode_payload failed for %s frame", name)
        if on_error is not None:
            try:
                on_error(ftype, payload, exc)
            except Exception:
                log.exception("on_error handler raised")
        return False
    try:
        on_frame(ftype, payload, decoded)
    except Exception:  # callback bug — log and keep going
        log.exception("on_frame callback raised")
        return False
    return True


class FakeSerial:
    """Minimal in-memory pyserial stand-in for tests (and replay).

    Implements just enough of the ``serial.Serial`` surface that
    :class:`SerialReader` uses: ``read(n)``, ``write(b)``, ``close()`` and the
    ``is_open`` attribute. Bytes to be "received" are queued with
    :meth:`feed`; bytes written by the reader land in :attr:`written`.

    Set ``raise_after`` to simulate the board browning out / re-enumerating:
    after that many ``read`` calls, the next ``read`` raises ``raise_exc``
    (default :class:`SerialError`, mimicking ``serial.SerialException``).
    """

    def __init__(self, data=b"", raise_after=None, raise_exc=None):
        self._rx = bytearray(data)
        self.written = bytearray()
        self.is_open = True
        self.closed = False
        self.read_calls = 0
        self.raise_after = raise_after
        self.raise_exc = raise_exc or SerialError("FakeSerial: link lost")

    def feed(self, data):
        """Queue more bytes to be returned by subsequent ``read`` calls."""
        self._rx += bytes(data)

    def read(self, n=1):
        """Return up to ``n`` queued bytes (may return b'' if drained)."""
        if self.closed:
            raise SerialError("read on closed FakeSerial")
        self.read_calls += 1
        if self.raise_after is not None and self.read_calls > self.raise_after:
            raise self.raise_exc
        if not self._rx:
            return b""
        take = self._rx[:n]
        del self._rx[:n]
        return bytes(take)

    def write(self, data):
        """Record written bytes; return the count, like pyserial."""
        if self.closed:
            raise SerialError("write on closed FakeSerial")
        self.written += bytes(data)
        return len(data)

    def flush(self):
        pass

    def close(self):
        self.closed = True
        self.is_open = False

    # context-manager sugar (handy in ad-hoc scripts/tests)
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class SerialReader:
    """Read frames off a serial port (or replay file) and dispatch them.

    Parameters
    ----------
    on_frame:
        Callback ``on_frame(frame_type, payload, decoded_dict)`` invoked for
        every CRC-valid frame. Exceptions raised inside it are caught and
        logged — they never break the loop.
    port, baud:
        Real-port settings. ``port`` may be ``None`` to auto-detect via
        :func:`find_base_port` using ``port_globs``.
    port_globs:
        Glob patterns for auto-detect (default ACM then USB).
    serial_obj:
        Dependency-injection seam: an already-open serial-like object (e.g.
        :class:`FakeSerial`). When given, the reader uses it directly instead
        of opening a real port. Combine with ``open_serial`` to control what a
        *reconnect* produces.
    open_serial:
        Optional zero-arg factory returning a fresh serial-like object. Called
        on first connect (if no ``serial_obj``) and on every reconnect. This is
        how tests inject a second :class:`FakeSerial` for the reconnect case.
    replay_path:
        If set, the reader runs in REPLAY mode: :meth:`run` streams the file
        through the pipeline and returns. (See also :func:`replay_bytelog`.)
    read_size:
        Bytes requested per ``read`` call.
    backoff_base, backoff_max:
        Reconnect backoff in seconds (exponential, capped at ``backoff_max``).
    on_error:
        Optional ``on_error(frame_type, payload, exc)`` for decoder failures.
    sleep:
        Injectable sleep function (tests pass a no-op for determinism).
    """

    def __init__(
        self,
        on_frame,
        port=DEFAULT_PORT,
        baud=DEFAULT_BAUD,
        port_globs=PORT_GLOBS,
        serial_obj=None,
        open_serial=None,
        replay_path=None,
        read_size=4096,
        backoff_base=0.5,
        backoff_max=10.0,
        on_error=None,
        sleep=time.sleep,
    ):
        self.on_frame = on_frame
        self.port = port
        self.baud = baud
        self.port_globs = tuple(port_globs)
        self.replay_path = replay_path
        self.read_size = read_size
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.on_error = on_error
        self._sleep = sleep

        self._open_serial = open_serial
        self._ser = serial_obj
        self._parser = protocol.FrameParser()
        self._write_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

        # /health status
        self.connected = bool(serial_obj is not None and getattr(
            serial_obj, "is_open", True))
        self.frames_total = 0
        self.reconnects = 0
        self.last_error = None
        self._ever_connected = bool(self.connected)

    # -- connection management ----------------------------------------------

    def _open(self):
        """Open (or adopt) a serial-like object and mark connected.

        Resolution order: an injected ``open_serial`` factory, else an already
        injected ``serial_obj`` (first use only), else a real ``serial.Serial``
        opened on an auto-detected/configured port. pyserial is imported here,
        lazily, so the module needs no pyserial to import.
        """
        if self._open_serial is not None:
            self._ser = self._open_serial()
        elif self._ser is not None:
            pass  # adopt the injected object as-is
        else:
            import serial  # lazy: only needed for a real port

            dev = find_base_port(self.port, self.port_globs)
            if not dev:
                raise SerialError("no serial device found")
            self.port = dev
            self._ser = serial.Serial(dev, self.baud, timeout=0.2)
        self.connected = bool(getattr(self._ser, "is_open", True))
        if self.connected:
            log.info("serial connected: %s", self.port)
        return self._ser

    def _close(self):
        """Close the current port (best-effort) and mark disconnected."""
        self.connected = False
        ser, self._ser = self._ser, None
        if ser is not None:
            try:
                ser.close()
            except Exception:
                log.debug("error while closing serial", exc_info=True)

    def _reconnect(self):
        """Back off, then try to (re)open the port until it succeeds/stop.

        Exponential backoff capped at ``backoff_max``. Re-detects the device
        each attempt (the port may come back as a different /dev/ttyACM*).
        Returns True on success, False if asked to stop.
        """
        attempt = 0
        first_ever = (self.reconnects == 0 and not self._ever_connected)
        while not self._stop.is_set():
            if not (first_ever and attempt == 0):
                delay = min(self.backoff_base * (2 ** attempt), self.backoff_max)
                self._sleep(delay)
            try:
                self._open()
                if not first_ever:
                    self.reconnects += 1
                self._ever_connected = True
                return True
            except Exception as exc:
                self.last_error = repr(exc)
                log.warning("reconnect attempt %d failed: %s", attempt + 1, exc)
                attempt += 1
        return False

    # -- the read/parse cycle ------------------------------------------------

    def poll_once(self):
        """Do exactly one read+parse+dispatch cycle. Deterministic; testable.

        Opens the port lazily on first call. Reads one chunk, feeds the parser,
        and dispatches every recovered frame via ``on_frame``. A read error
        (port gone / SerialException / OSError) is caught: the port is closed,
        ``connected`` flips False, and the reader transparently reconnects
        before the next read — so the loop survives a board re-enumeration.

        Returns the number of frames delivered in this cycle.
        """
        # Reconnect at the *start* of a cycle if a prior read dropped the link.
        # This keeps ``connected`` observably False for the whole cycle in
        # which the read failed, and reconnects lazily before the next read.
        if self._ser is None or not self.connected:
            if not self._reconnect():
                return 0  # stop requested mid-reconnect

        try:
            chunk = self._ser.read(self.read_size)
        except _serial_exception_types() as exc:
            # The link dropped (brown-out / unplug). Close + flip connected
            # False now; the *next* poll_once will back off and reopen. The
            # loop survives — it never raises out of poll_once.
            self.last_error = repr(exc)
            log.warning("serial read failed (%s); will reconnect", exc)
            self._close()
            return 0

        if not chunk:
            return 0  # idle read (timeout) — normal, nothing to do

        delivered = 0
        for ftype, payload in self._parser.push(chunk):
            if _dispatch(self.on_frame, ftype, payload, self.on_error):
                delivered += 1
                self.frames_total += 1
        return delivered

    # -- run loops -----------------------------------------------------------

    def run(self):
        """Run until :meth:`stop`. In replay mode, stream the file and return.

        Live mode loops over :meth:`poll_once`. Replay mode feeds the byte-log
        through the same decode/dispatch path and returns when the file ends.
        """
        if self.replay_path is not None:
            return self._run_replay()
        while not self._stop.is_set():
            self.poll_once()

    def _run_replay(self):
        """Stream ``replay_path`` through the pipeline (no hardware)."""
        log.info("replay mode: %s", self.replay_path)
        parser = self._parser
        with open(self.replay_path, "rb") as fh:
            while not self._stop.is_set():
                chunk = fh.read(self.read_size)
                if not chunk:
                    break
                for ftype, payload in parser.push(chunk):
                    if _dispatch(self.on_frame, ftype, payload, self.on_error):
                        self.frames_total += 1
        return self.frames_total

    def start(self):
        """Run :meth:`run` in a background daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            return self._thread
        self._stop.clear()
        self._thread = threading.Thread(
            target=self.run, name="srt-serial-reader", daemon=True
        )
        self._thread.start()
        return self._thread

    def stop(self, join_timeout=2.0):
        """Signal the loop to stop, close the port, and join the thread."""
        self._stop.set()
        self._close()
        if self._thread is not None:
            self._thread.join(timeout=join_timeout)
            self._thread = None

    # alias for callers that prefer the longer name
    shutdown = stop

    # -- downlink ------------------------------------------------------------

    def send(self, frame_bytes):
        """Write a pre-encoded frame (e.g. from ``protocol.encode_set_*``).

        Behaviour when disconnected: **raises** :class:`NotConnectedError`.
        We deliberately do *not* silently queue control frames — a SET_ARMED /
        SET_EPOCH that is silently dropped is worse than a loud failure the
        caller can retry once HEALTH shows the link back. Writes are guarded by
        a lock so the downlink thread and the read loop can share the port.
        """
        if not self.connected or self._ser is None:
            raise NotConnectedError("serial port not connected")
        with self._write_lock:
            ser = self._ser
            if ser is None:
                raise NotConnectedError("serial port not connected")
            try:
                ser.write(bytes(frame_bytes))
                flush = getattr(ser, "flush", None)
                if flush:
                    flush()
            except _serial_exception_types() as exc:
                self.last_error = repr(exc)
                self._close()
                raise NotConnectedError("write failed: %r" % (exc,))

    # -- introspection -------------------------------------------------------

    def health(self):
        """Snapshot dict for the /health endpoint."""
        return {
            "connected": self.connected,
            "port": self.port,
            "frames_total": self.frames_total,
            "reconnects": self.reconnects,
            "last_error": self.last_error,
        }

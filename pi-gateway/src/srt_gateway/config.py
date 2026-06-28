"""config.py — Sail Race Tracker gateway configuration.

Configuration is loaded from (in increasing precedence):

  1. built-in defaults (the :class:`Config` dataclass field defaults),
  2. a TOML file (if a path is given and the file exists), read with the
     stdlib ``tomllib`` (Python 3.11+) — keys map 1:1 to ``Config`` fields,
  3. environment variables prefixed ``SRT_GATEWAY_`` (e.g.
     ``SRT_GATEWAY_SERIAL_DEVICE``), which override everything.

This keeps the Pi deployment simple: ship a ``/etc/srt-gateway/config.toml``
for the stable settings, and let systemd / an operator override a single value
(an API key, a device path) via an environment variable without editing files.

``serial_device`` accepts the literal string ``"auto"`` (or empty) to mean
"auto-detect the base node port" — :class:`~srt_gateway.serial_io.SerialReader`
is then constructed with ``port=None`` so :func:`serial_io.find_base_port`
scans ``/dev/ttyACM*`` then ``/dev/ttyUSB*``.

No third-party dependency: ``tomllib`` is stdlib on 3.11+. On older Pythons we
fall back to the third-party ``tomli`` if installed, else TOML files are
skipped (env + defaults still work) with a warning.
"""

import dataclasses
import logging
import os

__all__ = ["Config", "load_config", "DEFAULTS"]

log = logging.getLogger("srt_gateway.config")

# Env var prefix for overrides: SRT_GATEWAY_<FIELD_NAME_UPPER>.
ENV_PREFIX = "SRT_GATEWAY_"


@dataclasses.dataclass
class Config:
    """Resolved gateway configuration.

    Field names match TOML keys and (upper-cased, prefixed) env var names.
    """

    serial_device: str = "/dev/ttyACM1"   # "auto"/"" -> auto-detect
    serial_baud: int = 115200
    cloud_url: str = "http://localhost:8787"
    cloud_api_key: str = ""
    ingest_interval_s: float = 5.0        # uplink drain idle interval
    downlink_interval_s: float = 10.0     # how often to poll cloud race state
    db_path: str = "/home/base-node/srt-gateway/srt.db"
    health_port: int = 8080
    log_level: str = "INFO"

    @property
    def serial_port(self):
        """The port to hand SerialReader: ``None`` when auto-detect requested."""
        dev = (self.serial_device or "").strip()
        if dev == "" or dev.lower() == "auto":
            return None
        return dev


# Snapshot of defaults for docs / introspection.
DEFAULTS = dataclasses.asdict(Config())

# Per-field coercion from string (env/TOML) to the dataclass type.
_FIELD_TYPES = {f.name: f.type for f in dataclasses.fields(Config)}


def _load_toml(path):
    """Return a dict from a TOML file, or {} if unavailable/missing."""
    if not path or not os.path.exists(path):
        return {}
    try:
        import tomllib  # Python 3.11+
        _loads = tomllib.load
        mode = "rb"
    except ModuleNotFoundError:
        try:
            import tomli  # backport for <3.11
            _loads = tomli.load
            mode = "rb"
        except ModuleNotFoundError:
            log.warning(
                "TOML config %s present but no tomllib/tomli available; "
                "ignoring file (env + defaults still apply)", path
            )
            return {}
    with open(path, mode) as fh:
        data = _loads(fh)
    # Only keep keys we know about; warn on the rest so typos are visible.
    known = set(_FIELD_TYPES)
    out = {}
    for k, v in data.items():
        if k in known:
            out[k] = v
        else:
            log.warning("unknown config key in %s: %r (ignored)", path, k)
    return out


def _coerce(name, value):
    """Coerce a raw value (often a string from env) to the field's type."""
    typ = _FIELD_TYPES.get(name)
    if value is None:
        return None
    if typ in (int, "int"):
        return int(value)
    if typ in (float, "float"):
        return float(value)
    # str (and everything else) passes through as text.
    return str(value)


def _env_overrides():
    """Collect SRT_GATEWAY_* env vars mapped onto Config field names."""
    out = {}
    for name in _FIELD_TYPES:
        env_key = ENV_PREFIX + name.upper()
        if env_key in os.environ:
            out[name] = os.environ[env_key]
    return out


def load_config(path=None):
    """Build a :class:`Config` from defaults <- TOML(path) <- env vars.

    ``path`` may be ``None`` (env + defaults only). Env (``SRT_GATEWAY_*``)
    always wins over the TOML file, which wins over the dataclass defaults.
    """
    values = {}
    values.update(_load_toml(path))
    values.update(_env_overrides())

    coerced = {}
    for name, raw in values.items():
        coerced[name] = _coerce(name, raw)

    cfg = Config(**coerced)
    return cfg

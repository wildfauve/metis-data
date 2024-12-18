import json
import logging
from pymonad.maybe import Just

from typing import Any, Callable
from pino import pino
import time

from .tracer import Tracer
from . import json_util, singleton


class NoopLogger():
    def __init__(self, level):
        self.logs = []
        self.level = level

    def info(self,
             meta,
             msg: str) -> None:
        if logging.INFO >= self.level:
            self.logs.append({**{"msg": msg, "level": 'info'}, **meta})
        pass

    def warn(self,
             meta,
             msg: str) -> None:
        if logging.WARN >= self.level:
            self.logs.append({**{"msg": msg, "level": 'warn'}, **meta})
        pass

    def debug(self,
              meta,
              msg: str) -> None:
        if logging.DEBUG >= self.level:
            self.logs.append({**{"msg": msg, "level": "debug"}, **meta})
        pass

    def error(self,
              meta,
              msg: str) -> None:
        if logging.ERROR >= self.level:
            self.logs.append({**{"msg": msg, "level": "error"}, **meta})
        pass



    def pp(self):
        logs = [json.dumps(log, indent=4) for log in self.logs]
        print("\n".join(logs))


@singleton.singleton
class LogConfig:
    _default_level: int = logging.INFO

    def __init__(self,
                 logger: Any = None,
                 level: int = logging.INFO):
        self._setup(logger, level)

    def _setup(self, logger, level):
        self.level = level if level else self.__class__._default_level
        self.logger = logger if logger else self._noop_logger(self.level)

    def reset(self, logger=None, level=None):
        self._setup(logger, level)
        return self

    def _noop_logger(self, level):
        return NoopLogger(level)

    def _standard_logger(self, level):
        return pino(bindings={"apptype": "prototype", "context": "main"},
                    dump_function=custom_pino_dump_fn,
                    level=level)

    def __repr__(self):
        return f"LogConfig(logger={self.logger}, level={self.level})"


def info(msg: str,
         ctx: dict | None = None,
         status: str = 'ok',
         **kwargs) -> None:
    _log('info', msg, status, ctx if ctx else {}, **kwargs)


def debug(msg: str,
          ctx: dict | None = None,
          status: str = 'ok',
          **kwargs) -> None:
    _log('debug', msg, status, ctx if ctx else {}, **kwargs)


def maybe_debug(msg: str,
                value: str,
                ctx: dict | None = None,
                status: str = 'ok') -> Just:
    """
    Logs at DEBUG level, including the value in the message. It then returns the value arg, which must be the 2nd arg,
    wrapped in a Just.  Does not support **kwargs for additional context.
    """
    _log('debug', f"{msg} : {str(value)}", status, ctx if ctx else {})
    return Just(value)


def maybe_info(msg: str,
               value: Any,
               ctx: dict | None = None,
               status: str = 'ok') -> Just:
    """
    Logs at INFO level and returns the value arg, which must be the 2nd arg, wrapped in a Just
    Does not support **kwargs for additional context.
    """
    _log('debug', f"{msg} : {str(value)}", status, ctx if ctx else {})
    return Just(value)


def warn(msg: str,
         ctx: dict | None = None,
         status: str = 'ok',
         **kwargs) -> None:
    _log('warning', msg, status, ctx if ctx else {}, **kwargs)


def error(msg: str,
          ctx: dict | None = None,
          status: str = 'ok',
          **kwargs) -> None:
    _log('error', msg, status, ctx if ctx else {}, **kwargs)


def _log(level: str,
         msg: str,
         status: str,
         ctx: dict[str, str],
         **kwargs) -> None:
    if level not in level_functions.keys():
        return
    level_functions.get(level, info)(configured_logger(), msg, meta(status, ctx, **kwargs))


def with_perf_log(perf_log_type: str = None, name: str = None):
    """
    Decorator which wraps the fn in a timer and writes a performance log
    """

    def inner(fn):
        def invoke(*args, **kwargs):
            t1 = time.time()
            result = fn(*args, **kwargs)
            t2 = time.time()
            if perf_log_type == 'http' and 'name' in kwargs:
                fn_name = kwargs['name']
            else:
                fn_name = name or fn.__name__
            perf_log(fn=fn_name, delta_t=(t2 - t1) * 1000.0)
            return result

        return invoke

    return inner


def log_decorator(fn):
    def log_writer(*args, **kwargs):
        _log(
            level='info',
            msg='Handling Command {fn}'.format(fn=fn.__name__),
            ctx=args[0].event,
            tracer=args[0].tracer
        )
        return fn(*args, **kwargs)

    return log_writer


def custom_pino_dump_fn(json_log):
    return json.dumps(json_log, cls=json_util.CustomLogEncoder)


def configured_logger():
    cfg = LogConfig()
    if cfg.logger:
        return cfg.logger
    cfg(level=logging.INFO)
    return cfg.logger


def _info(lgr, msg: str, meta: dict) -> None:
    lgr.info(meta, msg)


def _debug(lgr, msg: str, meta: dict) -> None:
    lgr.debug(meta, msg)


def _warn(lgr, msg: str, meta: dict) -> None:
    lgr.warn(meta, msg)


def _error(lgr, msg: str, meta: dict) -> None:
    lgr.error(meta, msg)


def perf_log(fn: str, delta_t: float, callback: Callable = None):
    if callback:
        callback(fn, delta_t)
    info("PerfLog", ctx={'fn': fn, 'delta_t': delta_t})


def meta(status: str | int, ctx: dict, **kwargs):
    return {**{'ctx': ctx, 'status': status},
            **kwargs}


level_functions = {'info': _info, 'error': _error, 'warn': _warn, 'debug': _debug}

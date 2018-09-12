import logging
import tqdm


class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        self.format = logging.Formatter(logging.BASIC_FORMAT).format
        super(self.__class__, self).__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def logger(level, name=None):
    name = name or __name__
    log = logging.getLogger(name)
    log.addHandler(TqdmLoggingHandler())
    log.setLevel(getattr(logging, level))

    return log

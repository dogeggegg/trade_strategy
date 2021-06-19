import sys
import logging
import time
import datetime
import os

from logging.handlers import RotatingFileHandler


from config.config import Config


__all__ = [
    'logger',
    'printf',
    'printe',
    'printw',
    'to_csv'
]


class LogFormatter(logging.Formatter):

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            # t = time.strftime(self.default_time_format, ct)
            # s = self.default_msec_format % (t, record.msecs)
            s = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return s


class LogStreamHandler(logging.StreamHandler):

    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            if record.levelno == logging.WARNING:
                msg = '\033[0;33m' + msg + self.terminator + '\033[0m'
            elif record.levelno == logging.ERROR:
                msg = '\033[0;31m' + msg + self.terminator + '\033[0m'
            else:
                msg = msg + self.terminator
            stream.write(msg)
            self.flush()
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)


class Logger:

    logger = logging.getLogger('log')
    logger.setLevel(logging.INFO)

    def add_file_handler(self, file_path):
        file_handler = RotatingFileHandler(file_path, maxBytes=10 * 1024)
        logformatter = LogFormatter('[%(asctime)s][%(levelname)0.1s]%(message)s')
        file_handler.setFormatter(logformatter)
        self.logger.addHandler(file_handler)

    def remove_file_handler(self, file_path):
        for handle in self.logger.handlers:
            base_filename = getattr(handle, 'baseFilename', None)
            if base_filename == file_path:
                self.logger.removeHandler(handle)

    def add_stream_handler(self):
        stream_handler = LogStreamHandler(sys.stdout)
        logformatter = LogFormatter('[%(asctime)s][%(levelname)0.1s]%(message)s')
        stream_handler.setFormatter(logformatter)
        self.logger.addHandler(stream_handler)

    @staticmethod
    def get_sys_frame(msg):
        f_back = sys._getframe().f_back.f_back.f_back
        file_name = os.path.splitext(os.path.split(f_back.f_code.co_filename)[-1])[0]
        fun_name = f_back.f_code.co_name
        fun_line = f_back.f_lineno
        f_back_info = f"[{file_name}.{fun_name}.{fun_line}]"
        msg = f"{f_back_info:>30}:{msg}"
        return msg

    def printf(self, msg, trace):
        if trace:
            msg = self.get_sys_frame(msg)
        self.logger.info(msg)

    def printw(self, msg, trace):
        if trace:
            msg = self.get_sys_frame(msg)
        self.logger.warning(msg)

    def printe(self, msg, trace):
        if trace:
            msg = self.get_sys_frame(msg)
        self.logger.error(msg)


logger = Logger()
logger.add_stream_handler()


def printf(msg, trace=True):
    logger.printf(msg, trace)


def printw(msg, trace=True):
    logger.printw(msg, trace)


def printe(msg, trace=True):
    logger.printe(msg, trace)


class ToFile(object):

    @staticmethod
    def to_csv(df, csv_name):
        csv_path = os.path.join(Config.ResultDatetimePath, csv_name + '.csv')
        df.to_csv(csv_path, index=None)


to_file = ToFile()


def to_csv(df, csv_name):
    to_file.to_csv(df, csv_name)


if __name__ == '__main__':
    printf('printf')
    printw('printw')
    printe('printe')

import logging
import os
import sys
from fsync.conf import SynConfig

class ColouredHandler(logging.StreamHandler):
    """ A colorful logging handler optimized for terminal debugging aesthetics.
    - Designed for diagnosis and debug mode output - not for disk logs
    - Highlight the content of logging message in more readable manner
    - Show function and line, so you can trace where your logging messages
      are coming from
    - Keep timestamp compact
    - Extra module/function output for traceability
    The class provide few options as member variables you
    would might want to customize after instiating the handler.
    """

    color_map = {
        'black'   : 0,
        'red'     : 1,
        'green'   : 2,
        'yellow'  : 3,
        'blue'    : 4,
        'magenta' : 5,
        'cyan'    : 6,
        'white'   : 7,
    }
    (csi, reset) = ('\x1b[', '\x1b[0m')

    #: Show logger name
    show_name = True

    #: (Default) format string, w/o color code
    _fmt = "[%(asctime)s] [%(levelname)s] %(message)s"

    #: Color of each column
    _column_color = {}

    def __init__(
        self, stream=sys.stdout,
        datefmt='%H:%M:%S',
        color_message_debug    = ('cyan'  , None , False),
        color_message_info     = ('white' , None , False),
        color_message_warning  = ('yellow', None , True),
        color_message_error    = ('red'   , None , True),
        color_message_critical = ('white' , 'red', True),
    ):
        """Construct colorful stream handler

        :param stream:  a stream to emit log (e.g. sys.stderr, sys.stdout, writable `file` object, ...)
        :type color_*:  str compatible to `time.strftime()` argument
        :param datefmt: format of %(asctime)s, passed to `logging.Formatter.__init__()`.
            If `None` is passed, `logging`'s default format of '%H:%M:%S,<milliseconds>' is used.
        :type color_*:  `(<symbolic name of foreground color>, <symbolic name of background color>, <brightness flag>)`
        :param color_*: Each column's color. See `logging.Formatter` for supported column (`*`)
        """
        super().__init__(stream)

        # set timestamp format
        self._datefmt = datefmt

        # set custom color
        self._column_color[logging.DEBUG]    = color_message_debug
        self._column_color[logging.INFO]     = color_message_info
        self._column_color[logging.WARNING]  = color_message_warning
        self._column_color[logging.ERROR]    = color_message_error
        self._column_color[logging.CRITICAL] = color_message_critical

    @property
    def is_tty(self):
        """Returns true if the handler's stream is a terminal."""
        return getattr(self.stream, 'isatty', lambda: False)()

    def get_color(self, fg=None, bg=None, bold=False):
        """
        Construct a terminal color code

        :param fg: Symbolic name of foreground color
        :param bg: Symbolic name of background color
        :param bold: Brightness bit
        """
        params = []
        if bg in self.color_map:
            params.append(str(self.color_map[bg] + 40))
        if fg in self.color_map:
            params.append(str(self.color_map[fg] + 30))
        if bold:
            params.append('1')

        color_code = ''.join((self.csi, ';'.join(params), 'm'))

        return color_code

    def colorize(self, record):
        """
        Get a special format string with ASCII color codes.
        """
        color_fmt = self._colorize_fmt(self._fmt, record.levelno)
        formatter = logging.Formatter(color_fmt, self._datefmt)
        self.colorize_traceback(formatter, record)
        output = formatter.format(record)
        # Clean cache so the color codes of traceback don't leak to other formatters
        record.ext_text = None
        return output

    def colorize_traceback(self, formatter, record):
        """
        Turn traceback text to red.
        """
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            record.exc_text = "".join([
                self.get_color("red"),
                formatter.formatException(record.exc_info),
                self.reset,
            ])

    def format(self, record):
        """
        Formats a record for output.
        Takes a custom formatting path on a terminal.
        """
        if self.is_tty:
            message = self.colorize(record)
        else:
            message = logging.StreamHandler.format(self, record)

        return message

    def emit(self, record):
        """Emit colorized `record` when called from `logging` module's printing functions"""
        try:
            msg = self.format(record)
            msg = self._encode(msg)
            self.stream.write(msg + getattr(self, 'terminator', '\n'))
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def setFormatter(self, formatter):
        # HACK: peeping format string passed by user to `logging.Formatter()`
        if formatter._fmt:
            self._fmt = formatter._fmt
        logging.StreamHandler.setFormatter(self, formatter)

    def _encode(self, msg):
        """Encode `msg` if it is `unicode` object"""
        if (2, 6, 0) <= sys.version_info < (3, 0, 0) and unicode and isinstance(msg, unicode):
            enc = getattr(self.stream, 'encoding', 'utf-8')
            if enc is None:
                # An encoding property was found, but it was None
                enc = 'utf-8'
            return msg.encode(enc, 'replace')
        return msg

    def _colorize_fmt(self, fmt, levelno):
        """Adds ANSI color codes on plain `fmt`"""
        color_tup = self._column_color[levelno]
        fmt = ''.join([self.reset, self.get_color(*color_tup), fmt, self.reset])
        return fmt

def get_logger():
    logger = logging.getLogger('log')
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        # logging format
        fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(filename)s:%(lineno)d %(message)s", "%H:%M:%S")

        if SynConfig.config['log'] != '':
            # filehandler
            fh = logging.FileHandler(SynConfig.config['log'])
            fh.setFormatter(fmt)
            fh.setLevel(logging.DEBUG)
            logger.addHandler(fh)

        # streamhandler
        if os.name == 'nt': # Windows
            ch= logging.StreamHandler()
        else:
            ch = ColouredHandler()
        ch.setFormatter(fmt)
        #ch.setLevel(logging.INFO)
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)

    return logger

logger = get_logger()

if __name__ == "__main__":
    def test_usage():
        logger = logging.getLogger('test_logging')
        logger.setLevel(logging.DEBUG)
        handler = ColouredHandler()
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(filename)s:%(lineno)d %(message)s", "%H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.debug("debug msg")
        logger.info("info msg")
        logger.warn("warn msg")
        logger.error("error msg")
        logger.critical("critical msg")

        try:
            raise RuntimeError("Opa!")
        except Exception as e:
            logger.exception(e)

    def test_unicode():
        logger = logging.getLogger('test_unicode')
        logger.setLevel(logging.DEBUG)
        handler = ColouredHandler()
        logger.addHandler(handler)
        logger.debug('日本語ログ')



    test_usage()
    test_unicode()
    logger.debug("debug msg")
    logger.info("info msg")
    logger.warn("warn msg")
    logger.error("error msg")
    logger.critical("critical msg")
    logger.debug('日本語ログ')


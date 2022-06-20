import logging
import logging.config


def loggers_init(settings):
    logging.config.dictConfig(settings.log_config)

    root = logging.getLogger()
    if not root.hasHandlers():
        fmtr = logging.Formatter(
            fmt="[%(asctime)s] [%(process)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S %z",
        )
        hdlr = logging.StreamHandler()
        hdlr.setFormatter(fmtr)
        root.addHandler(hdlr)

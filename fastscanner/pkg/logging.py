import logging
import logging.config

import yaml


def load_logging_config():
    with open("logging.yaml", "r") as log_config_file:
        config = yaml.safe_load(log_config_file)
    logging.config.dictConfig(config)
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)

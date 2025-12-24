import os

from dotenv import load_dotenv

load_dotenv()

SERVER_HOST = "localhost"
SERVER_PORT = 12356
DEBUG = bool(int(os.getenv("DEBUG", "0")))

# Massive (Previously Polygon.io)
POLYGON_BASE_URL = os.environ["POLYGON_BASE_URL"]
POLYGON_API_KEY = os.environ["POLYGON_API_KEY"]
MASSIVE_FILES_BASE_URL = os.environ["MASSIVE_FILES_BASE_URL"]
MASSIVE_FILES_ACCESS_KEY = os.environ["MASSIVE_FILES_ACCESS_KEY"]
MASSIVE_FILES_SECRET_KEY = os.environ["MASSIVE_FILES_SECRET_KEY"]

# EODHD
EOD_HD_BASE_URL = os.environ["EOD_HD_BASE_URL"]
EOD_HD_API_KEY = os.environ["EOD_HD_API_KEY"]

# Redis
REDIS_DB_PORT = 6379
REDIS_DB_HOST = "localhost"
UNIX_SOCKET_PATH = os.environ.get("REDIS_UNIX_SOCKET", "/tmp/redis-server.sock")

# NATS
NATS_SERVER = os.environ.get("NATS_SERVER", "nats://localhost:4222").split(",")
NATS_SYMBOL_SUBSCRIBE_CHANNEL = os.environ.get(
    "NATS_SYMBOL_SUBSCRIBE_CHANNEL", "symbol_subscribe"
)
NATS_SYMBOL_UNSUBSCRIBE_CHANNEL = os.environ.get(
    "NATS_SYMBOL_UNSUBSCRIBE_CHANNEL", "symbol_unsubscribe"
)

# Data cache directory
DATA_BASE_DIR = os.environ["DATA_BASE_DIR"]
TRADES_DATA_DIR = os.environ["TRADES_DATA_DIR"]

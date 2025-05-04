import os

from dotenv import load_dotenv

load_dotenv()

SERVER_PORT = 12356
DEBUG = bool(int(os.getenv("DEBUG", "0")))
POLYGON_BASE_URL = os.environ["POLYGON_BASE_URL"]
POLYGON_API_KEY = os.environ["POLYGON_API_KEY"]
EOD_HD_BASE_URL = os.environ["EOD_HD_BASE_URL"]
EOD_HD_API_KEY = os.environ["EOD_HD_API_KEY"]
REDIS_DB_PORT = 6379
REDIS_DB_HOST = "localhost"
INDICATORS_CALCULATE_RESULTS_DIR = "output/indicator_results"

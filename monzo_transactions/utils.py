import logging
import os
import sys
import datetime

if logging.getLogger().hasHandlers():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
else:
    os.makedirs(os.path.join(os.getcwd(), "logs"), exist_ok=True)
    file_handler = logging.FileHandler(filename="./logs/log.log")
    file_formatter = logging.Formatter(
        "[%(asctime)s] - %(name)s - %(levelname)s - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_formatter = logging.Formatter(
        "[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    stdout_handler.setFormatter(stdout_formatter)
    handlers = [file_handler, stdout_handler]
    logging.basicConfig(handlers=handlers)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)


def get_timestamp(string=False):
    ts = datetime.datetime.today()
    if string:
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return ts


TODAY = datetime.date.today()
PAST_THREE_MONTHS = datetime.datetime.today() - datetime.timedelta(days=89)
TOKENS = "/home/ubuntu/airflow/dags/monzo_transactions/tokens"
MONZO_AUTH_PICKLE = "/home/ubuntu/airflow/dags/monzo_transactions/monzo_auth.pickle"
TRANSACTIONS_JSON = "/home/ubuntu/airflow/dags/monzo_transactions/transactions.json"
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
REDIRECT_URI = "http://127.0.0.1/monzo"

import os
import datetime
from json import loads

from airflow.decorators import dag, task
from airflow_provider_hightouch.operators.hightouch import HightouchTriggerSyncOperator

from monzo_balances.source_accounts import SOURCE_CURRENT_ACCOUNT
from monzo_transactions.utils import TOKENS, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI


class MonzoException(Exception):
    pass


default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule_interval="*/10 7-23 * * *",
    start_date=datetime.datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def monzo_balances():
    @task()
    def get_monzo_auth():

        from monzo.authentication import Authentication
        from monzo.handlers.filesystem import FileSystem

        with open(
            TOKENS,
            "r",
        ) as tokens:
            content = loads(tokens.read())

        monzo_auth_obj = Authentication(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_url=REDIRECT_URI,
            access_token=content["access_token"],
            access_token_expiry=content["expiry"],
            refresh_token=content["refresh_token"],
        )
        handler = FileSystem(TOKENS)
        monzo_auth_obj.register_callback_handler(handler)

        return monzo_auth_obj

    @task()
    def get_monzo_balances(monzo_auth_obj):

        from monzo_balances.fetch_pots import FetchPots

        pots_path = "/home/ubuntu/airflow/dags/monzo_balances/pots.json"

        get_pots = FetchPots(monzo_auth_obj)

        all_pots = get_pots.fetch_pots(SOURCE_CURRENT_ACCOUNT)

        all_pots.to_json(pots_path, orient="records")

        return pots_path

    @task()
    def upload_monzo_balances(pots_path):

        import pandas as pd

        from monzo_balances.upload_balances import UploadBalances

        with open(pots_path, "rb") as pots:
            final_pots = loads(pots.read())

        final_pots_df = pd.DataFrame(final_pots)

        upload = UploadBalances(
            balances=final_pots_df, schema="public", table="balances"
        )

        upload.get_new_balances()
        num_of_changed_trans = len(upload.get_changed_balances())

        try:
            upload.upload_new_balances()
        except:
            raise MonzoException("An error occured while uploading new balances")

        if num_of_changed_trans > 0:

            try:
                upload.update_changed_balances()
            except:
                raise MonzoException("An error occured while updating balances")

    notion_sync = HightouchTriggerSyncOperator(
        task_id="run_notion_sync",
        sync_id=38788,
        synchronous=True,
        error_on_warning=True,
    )

    monzo_api_authentication = get_monzo_auth()
    pull_balances = get_monzo_balances(monzo_api_authentication)
    (upload_monzo_balances(pull_balances) >> notion_sync)


dag = monzo_balances()

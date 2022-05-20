import os
import datetime
from json import loads

from airflow.decorators import dag, task
from airflow_provider_hightouch.operators.hightouch import HightouchTriggerSyncOperator

from monzo_transactions.source_accounts import SOURCE_ACCOUNTS
from monzo_transactions.utils import TOKENS, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI


class MonzoException(Exception):
    pass


default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule_interval="5 8,14,20 * * *",
    start_date=datetime.datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def monzo_transactions():
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
    def get_monzo_transactions(monzo_auth_obj):

        import pandas as pd

        from monzo_transactions.fetch_transactions import FetchTransactions

        transactions_path = (
            "/home/ubuntu/airflow/dags/monzo_transactions/transactions.json"
        )

        trn = FetchTransactions(monzo_auth_obj)

        all_transactions = []
        for id in SOURCE_ACCOUNTS:
            transactions = trn.fetch_transactions(id)
            all_transactions.append(transactions)

        final_transactions = pd.concat(all_transactions)
        final_transactions.to_json(transactions_path, orient="records")

        return transactions_path

    @task()
    def upload_monzo_transactions(transactions_path, monzo_auth_obj):

        import pandas as pd
        from monzo.endpoints.feed_item import FeedItem
        from monzo.exceptions import MonzoError

        from monzo_transactions.upload_transactions import UploadTransactions

        with open(transactions_path, "rb") as transactions:
            final_transactions = loads(transactions.read())

        final_transactions_df = pd.DataFrame(final_transactions)

        final_transactions_df["date"] = pd.to_datetime(
            final_transactions_df["date"], unit="ms"
        )
        final_transactions_df["created"] = pd.to_datetime(
            final_transactions_df["created"]
        )

        upload = UploadTransactions(
            transactions=final_transactions_df, schema="public", table="transactions"
        )

        new_transactions = upload.get_new_transactions()
        changed_transactions = upload.get_changed_transactions()

        num_of_new_trans = len(new_transactions)
        num_of_changed_trans = len(changed_transactions)

        lst_new_trans_desc = list(new_transactions.merchant_description)
        lst_changed_trans_desc = list(changed_transactions.merchant_description)

        try:
            upload.upload_new_transactions()
        except:
            raise MonzoException("An error occured while uploading new transactions")

        params = {
            "title": "New Transactions Inserted",
            "image_url": "https://monzo.com/static/images/favicon.png",
            "body": f"{num_of_new_trans} new transactions inserted: \n\n {str(lst_new_trans_desc).replace('[', '').replace(']', '')}",
            "title_color": "#9cb4b3",
        }

        if num_of_new_trans > 0:
            try:
                FeedItem.create(
                    monzo_auth_obj, SOURCE_ACCOUNTS[0], "basic", params, None
                )
            except MonzoError:
                print("Failed to create feed item for new transactions")

        if num_of_changed_trans > 0:
            try:
                upload.update_changed_transactions()
            except:
                raise MonzoException("No changed transactions to update")

            params = {
                "title": "Transactions Updated",
                "image_url": "https://monzo.com/static/images/favicon.png",
                "body": f"{num_of_changed_trans} transactions updated: \n\n {str(lst_changed_trans_desc).replace('[', '').replace(']', '')}",
            }

            try:
                FeedItem.create(
                    monzo_auth_obj, SOURCE_ACCOUNTS[0], "basic", params, None
                )
            except MonzoError:
                print("Failed to create feed item for updated transactions")

    notion_sync = HightouchTriggerSyncOperator(
        task_id="run_notion_sync",
        sync_id=35985,
        synchronous=True,
        error_on_warning=True,
    )

    monzo_api_authentication = get_monzo_auth()
    pull_transactions = get_monzo_transactions(monzo_api_authentication)
    (
        upload_monzo_transactions(pull_transactions, monzo_api_authentication)
        >> notion_sync
    )


dag = monzo_transactions()

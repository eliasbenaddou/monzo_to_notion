import logging

import numpy as np
import pandas as pd
from monzo.endpoints.transaction import Transaction

from monzo_transactions.categories import cat_dict, columns_dict

from .utils import PAST_THREE_MONTHS, TODAY

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class FetchTransactions:
    """Class to get transactions from Monzo using monzo-api package by Peter Mcdonald"""

    def __init__(self, monzo_auth=None):

        self.monzo_auth = monzo_auth

    def fetch_transactions(
        self,
        source_account,
    ):

        fetched_transactions = Transaction.fetch(
            auth=self.monzo_auth,
            account_id=source_account,
            since=PAST_THREE_MONTHS,
        )

        transactions_dict = {}

        for index, _ in enumerate(fetched_transactions):
            transaction_data = Transaction.fetch_single(
                auth=self.monzo_auth,
                transaction_id=fetched_transactions[index].transaction_id,
            )

            id = transaction_data.transaction_id
            scheme = transaction_data.scheme
            date = transaction_data.created
            description = transaction_data.description
            amount = transaction_data.amount
            category = transaction_data.category
            categories = transaction_data.categories
            counterparty = transaction_data.counterparty
            decline_reason = transaction_data.decline_reason
            merchant = transaction_data.merchant
            meta = transaction_data.metadata

            transactions_dict[index] = (
                id,
                date,
                description,
                amount,
                category,
                categories,
                counterparty,
                scheme,
                decline_reason,
                merchant,
                meta,
            )

        transactions_df = pd.DataFrame(transactions_dict).T
        transactions_df.rename(
            columns={
                0: "id",
                1: "date",
                2: "description",
                3: "amount",
                4: "category",
                5: "categories",
                6: "counterparty",
                7: "scheme",
                8: "decline_reason",
                9: "merchant_info",
                10: "meta",
            },
            inplace=True,
        )

        if len(transactions_df) > 0:

            transactions_df["decline"] = np.where(
                transactions_df["decline_reason"] == "", 0, 1
            )

            merchant_info = pd.json_normalize(transactions_df["merchant_info"])
            metadata = pd.json_normalize(transactions_df["meta"])
            multiple_categories = pd.json_normalize(transactions_df["categories"])

            transactions_merchant = transactions_df.merge(
                merchant_info, left_index=True, right_index=True
            )
            transactions_meta = transactions_merchant.merge(
                metadata, left_index=True, right_index=True
            )
            transactions_multiple_categories = transactions_meta.merge(
                multiple_categories, left_index=True, right_index=True
            )
            transactions = transactions_multiple_categories.drop(
                ["merchant_info", "meta", "categories", "counterparty"], axis=1
            )

            if "category_x" in transactions.columns:
                transactions["category_x"].replace(cat_dict, inplace=True)
                transactions["amount"] = (
                    transactions["amount"].apply(lambda x: -1 * (x / 100)).astype(float)
                )
                transactions = transactions.assign(origin="monzo")
                transactions.reset_index(drop=True, inplace=True)
                transactions.rename(columns=columns_dict, inplace=True)
                if "updated" not in transactions.columns:
                    transactions["created"] = TODAY
                    transactions["amount"] = pd.to_numeric(
                        transactions["amount"], downcast="float"
                    )
                else:
                    transactions = transactions.drop(["updated"], axis=1)
                    transactions["created"] = TODAY
                    transactions["amount"] = pd.to_numeric(
                        transactions["amount"], downcast="float"
                    )

            else:
                transactions["category"].replace(cat_dict, inplace=True)
                transactions["amount"] = (
                    transactions["amount"].apply(lambda x: -1 * (x / 100)).astype(float)
                )
                transactions = transactions.assign(origin="monzo")
                transactions.reset_index(drop=True, inplace=True)
                transactions.rename(columns=columns_dict, inplace=True)
                if "updated" not in transactions.columns:
                    transactions["created"] = TODAY
                    transactions["amount"] = pd.to_numeric(
                        transactions["amount"], downcast="float"
                    )
                else:
                    transactions = transactions.drop(["updated"], axis=1)
                    transactions["created"] = TODAY
                    transactions["amount"] = pd.to_numeric(
                        transactions["amount"], downcast="float"
                    )

        else:
            transactions = None
        return transactions

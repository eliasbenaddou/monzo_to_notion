import numpy as np

from monzo_transactions import sql_templates
from monzo_transactions.categories import compare_transaction_cols, monzo_categories_lst
from monzo_transactions.db import FinancesDb


class UploadTransactions:
    """Class to upload new transactions or edit existing ones on the database"""

    def __init__(
        self,
        transactions,
        schema,
        table,
        changed_transactions=None,
        transactions_to_upload=None,
    ):

        self.transactions = transactions
        self.db = FinancesDb()
        self.schema = schema
        self.table = table
        self.schema_table = schema + "." + table
        self.changed_transactions = changed_transactions
        self.transactions_to_upload = transactions_to_upload

        for col in monzo_categories_lst:
            if col not in self.transactions.columns:
                self.transactions[col] = np.nan

    def _get_uploaded_transactions(self):

        uploaded_transactions = self.db.query(
            sql=sql_templates.exists.format(schema=self.schema, table=self.table),
            return_data=True,
        )

        return uploaded_transactions

    def _delete(self, data):

        sql_delete = sql_templates.delete.format(
            schema=self.schema, table=self.table, transactions=data
        )

        self.db.query(sql=sql_delete, return_data=False)

    def _insert(self, data):

        self.db.insert(self.schema_table, df=data)

    def get_new_transactions(self):

        uploaded_transactions = self._get_uploaded_transactions()

        uploaded_ids_lst = uploaded_transactions["id"].tolist()

        new_transaction_ids = []
        for item in self.transactions["id"].tolist():
            if item not in uploaded_ids_lst:
                new_transaction_ids.append(item)

        self.transactions_to_upload = self.transactions[
            self.transactions["id"].isin(new_transaction_ids)
        ].reset_index(drop=True)

        return self.transactions_to_upload

    def get_changed_transactions(self):

        uploaded_transactions = self._get_uploaded_transactions()

        uploaded_ids_lst = uploaded_transactions["id"].tolist()

        seen_transactions = self.transactions[
            self.transactions["id"].isin(uploaded_ids_lst)
        ]

        seen_transactions = seen_transactions[compare_transaction_cols]
        seen_transactions = seen_transactions.sort_values("id")
        seen_transactions = seen_transactions.reset_index(drop=True)
        seen_transactions = seen_transactions.replace({None: np.nan})
        seen_transactions = seen_transactions.fillna(0)
        seen_transactions["amount"] = seen_transactions["amount"].astype(float)
        seen_transactions["amount"] = seen_transactions["amount"].round(2)

        seen_transactions_ids = seen_transactions["id"].tolist()

        seen_uploaded_transactions = uploaded_transactions[
            uploaded_transactions["id"].isin(seen_transactions_ids)
        ]
        seen_uploaded_transactions = seen_uploaded_transactions.sort_values("id")
        seen_uploaded_transactions = seen_uploaded_transactions.reset_index(drop=True)
        seen_uploaded_transactions = seen_uploaded_transactions.replace({None: np.nan})
        seen_uploaded_transactions = seen_uploaded_transactions.fillna(0)
        seen_uploaded_transactions["amount"] = seen_uploaded_transactions[
            "amount"
        ].astype(float)
        seen_uploaded_transactions["amount"] = seen_uploaded_transactions[
            "amount"
        ].round(2)

        self.changed_transactions = seen_transactions[
            seen_uploaded_transactions.ne(seen_transactions).any(axis=1)
        ]

        return self.changed_transactions

    def upload_new_transactions(self):

        self._insert(self.transactions_to_upload)

    def update_changed_transactions(self):

        transactions_to_delete_ids = self.changed_transactions["id"].tolist()
        transactions_to_delete_ids_str = (
            str(self.changed_transactions["id"].tolist()).strip("[").strip("]")
        )

        self._delete(transactions_to_delete_ids_str)

        transactions_to_reinsert = self.transactions[
            self.transactions["id"].isin(transactions_to_delete_ids)
        ]

        self._insert(transactions_to_reinsert)

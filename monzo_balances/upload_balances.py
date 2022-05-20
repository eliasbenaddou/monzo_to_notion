import numpy as np

from monzo_balances import sql_templates
from monzo_transactions.db import FinancesDb


class UploadBalances:
    """Class to upload pot balances or edit existing ones on the database"""

    def __init__(
        self,
        balances,
        schema,
        table,
    ):

        self.balances = balances
        self.db = FinancesDb()
        self.schema = schema
        self.table = table
        self.schema_table = schema + "." + table
        self.balances_to_upload = None
        self.changed_balances = None

    def _get_uploaded_balances(self):

        uploaded_balances = self.db.query(
            sql=sql_templates.exists.format(schema=self.schema, table=self.table),
            return_data=True,
        )

        return uploaded_balances

    def _delete(self, data):

        sql_delete = sql_templates.delete.format(
            schema=self.schema, table=self.table, id=data
        )

        self.db.query(sql=sql_delete, return_data=False)

    def _insert(self, data):

        self.db.insert(self.schema_table, df=data)

    def get_new_balances(self):

        uploaded_balances = self._get_uploaded_balances()

        uploaded_ids_lst = uploaded_balances["id"].tolist()

        new_pot_ids = []
        for item in self.balances["id"].tolist():
            if item not in uploaded_ids_lst:
                new_pot_ids.append(item)

        self.balances_to_upload = self.balances[
            self.balances["id"].isin(new_pot_ids)
        ].reset_index(drop=True)

        return self.balances_to_upload

    def get_changed_balances(self):

        uploaded_balances = self._get_uploaded_balances()

        uploaded_ids_lst = uploaded_balances["id"].tolist()

        seen_balances = self.balances[self.balances["id"].isin(uploaded_ids_lst)]

        seen_balances = seen_balances.sort_values("id")
        seen_balances = seen_balances.reset_index(drop=True)
        seen_balances = seen_balances.replace({None: np.nan})
        seen_balances = seen_balances.fillna(0)
        seen_balances["balance"] = seen_balances["balance"].astype(float)
        seen_balances["balance"] = seen_balances["balance"].round(2)

        seen_balances_ids = seen_balances["id"].tolist()

        seen_uploaded_balances = uploaded_balances[
            uploaded_balances["id"].isin(seen_balances_ids)
        ]
        seen_uploaded_balances = seen_uploaded_balances.sort_values("id")
        seen_uploaded_balances = seen_uploaded_balances.reset_index(drop=True)
        seen_uploaded_balances = seen_uploaded_balances.replace({None: np.nan})
        seen_uploaded_balances = seen_uploaded_balances.fillna(0)
        seen_uploaded_balances["balance"] = seen_uploaded_balances["balance"].astype(
            float
        )
        seen_uploaded_balances["balance"] = seen_uploaded_balances["balance"].round(2)

        self.changed_balances = seen_balances[
            seen_uploaded_balances.ne(seen_balances).any(axis=1)
        ]

        return self.changed_balances

    def upload_new_balances(self):

        self._insert(self.balances_to_upload)

    def update_changed_balances(self):

        balances_to_delete_ids = self.changed_balances["id"].tolist()
        balances_to_delete_ids_str = (
            str(self.changed_balances["id"].tolist()).strip("[").strip("]")
        )

        self._delete(balances_to_delete_ids_str)

        balances_to_reinsert = self.balances[
            self.balances["id"].isin(balances_to_delete_ids)
        ]

        self._insert(balances_to_reinsert)

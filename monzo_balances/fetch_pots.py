import logging

import numpy as np
import pandas as pd
from monzo.endpoints.pot import Pot


class FetchPots:
    """Class to get balances from Monzo using the monzo-api package by Peter Mcdonald"""

    def __init__(self, monzo_auth=None):

        self.monzo_auth = monzo_auth

    def fetch_pots(
        self,
        source_account,
    ):

        fetched_pots = Pot.fetch(
            auth=self.monzo_auth,
            account_id=source_account,
        )

        pots_dict = {}

        for index, pot in enumerate(fetched_pots):

            id = pot.pot_id
            name = pot.name
            style = pot.style
            balance = pot.balance
            currency = pot.currency
            created = pot.created
            updated = pot.updated
            deleted = pot.deleted

            pots_dict[index] = (
                id,
                name,
                style,
                balance,
                currency,
                deleted,
            )

        pots_df = pd.DataFrame(pots_dict).T
        pots_df.rename(
            columns={
                0: "id",
                1: "name",
                2: "style",
                3: "balance",
                4: "currency",
                5: "deleted",
            },
            inplace=True,
        )

        pots_df["balance"] = pots_df["balance"].apply(lambda x: (x / 100)).astype(float)

        return pots_df

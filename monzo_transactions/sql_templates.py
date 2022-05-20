delete = """
DELETE FROM {schema}.{table} x
WHERE x.id IN ({transactions})
"""

exists = """
SELECT "id",
"date",
"description",
"amount",
"category",
"merchant_description",
"logo",
"emoji",
"merchant_category",
"notes",
"monzo_category_transport",
"monzo_category_transfers",
"monzo_category_groceries",
"monzo_category_crypto",
"monzo_category_subscriptions",
"monzo_category_eating_out",
"monzo_category_shopping",
"monzo_category_healthcare",
"monzo_category_pets",
"monzo_category_gifts",
"monzo_category_entertainment",
"monzo_category_bills",
"monzo_category_home",
"monzo_category_gym",
"monzo_category_income",
"monzo_category_clothes",
"monzo_category_haircut",
"monzo_category_refunds",
"monzo_category_education",
"monzo_category_fees",
"monzo_category_stocks",
"monzo_category_isa",
"monzo_category_withdrawals",
"monzo_category_savings",
"monzo_category_holidays"
FROM {schema}.{table} x
WHERE x.origin = 'monzo'
"""

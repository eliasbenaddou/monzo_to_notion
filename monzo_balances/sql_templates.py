delete = """
DELETE FROM {schema}.{table} x
WHERE x.id IN ({id})
"""

exists = """
SELECT
"id",
"name",
"style",
"balance",
"currency",
"deleted"
FROM {schema}.{table} x
"""

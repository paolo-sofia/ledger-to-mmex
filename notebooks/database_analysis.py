import sqlite3
from sqlite3 import Error

import pandas as pd

MMEX_PATH: str = "/home/paolo/Nextcloud/MoneyManager/finances.mmb"


def get_transactions() -> list:
    connection: sqlite3.Connection = sqlite3.connect(MMEX_PATH)
    cursor: sqlite3.Cursor = connection.cursor()

    return cursor.execute(
        """
        SELECT
            *
        FROM
            CHECKINGACCOUNT_V1
        """
    ).fetchall()


def get_column_names_table(table_name: str) -> list[str]:
    connection: sqlite3.Connection = sqlite3.connect(MMEX_PATH)
    cursor: sqlite3.Cursor = connection.cursor()
    cursor = cursor.execute(
        f"""
        SELECT
            *
        FROM
            {table_name}
        LIMIT 1
        """
    )
    column_names: list[str] = [description[0] for description in cursor.description]
    connection.close()
    return column_names


def update_row(conn: sqlite3.Connection, row: tuple[str, str]) -> None:
    sql: str = """
    UPDATE
        CHECKINGACCOUNT_V1
    SET
        TRANSID = ?
    WHERE
        TRANSID = ?
            """
    cursor: sqlite3.Cursor = connection.cursor()
    cursor.execute(sql, row)
    connection.commit()


def create_connection(db_file: str) -> sqlite3.Connection | None:
    try:
        return sqlite3.connect(db_file)
    except Error:
        return None


column_names: list[str] = get_column_names_table("CHECKINGACCOUNT_V1")
transactions: pd.DataFrame = pd.DataFrame(get_transactions(), columns=column_names)
transactions = transactions.sort_values("TRANSDATE").reset_index(drop=True).reset_index(names="NEWTRANSID")


connection: sqlite3.Connection = create_connection(MMEX_PATH)
with connection:
    for _, row in transactions.sort_values("TRANSID").iterrows():
        update_row(connection, (-row["NEWTRANSID"], row["TRANSID"]))

    for _, row in transactions.sort_values("TRANSID").iterrows():
        update_row(connection, (row["NEWTRANSID"], -row["NEWTRANSID"]))

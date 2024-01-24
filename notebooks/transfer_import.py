import bisect
import pathlib
import sqlite3
from datetime import datetime

import pandas as pd
import pytz

MMEX_PATH: str = "/home/paolo/Nextcloud/MoneyManager/finances.mmb"
DATETIME_FORMAT: str = "%Y-%m-%dT%H:%M:%S"


def get_transactions_id() -> list[int]:
    with sqlite3.connect(MMEX_PATH) as connection:
        cursor: sqlite3.Cursor = connection.cursor()
        results: list = cursor.execute(
            """
            SELECT
                TRANSID
            FROM
                CHECKINGACCOUNT_V1
            """
        ).fetchall()

    return sorted([trans_id[0] for trans_id in results])


def get_column_names_table(table_name: str) -> list[str]:
    with sqlite3.connect(MMEX_PATH) as connection:
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
    return column_names


def load_accounts() -> pd.DataFrame:
    with sqlite3.connect(MMEX_PATH) as connection:
        cursor: sqlite3.Cursor = connection.cursor()
        results: list = cursor.execute(
            """
            SELECT
                ACCOUNTID, ACCOUNTNAME
            FROM
                ACCOUNTLIST_V1
            """
        ).fetchall()

        column_names: list[str] = [description[0] for description in cursor.description]

    return pd.DataFrame(results, columns=column_names)


def load_categories() -> pd.DataFrame:
    with sqlite3.connect(MMEX_PATH) as connection:
        cursor: sqlite3.Cursor = connection.cursor()
        results: list = cursor.execute(
            """
            SELECT
                CATEGID, CATEGNAME, PARENTID
            FROM
                CATEGORY_V1
            """
        ).fetchall()

        column_names: list[str] = [description[0] for description in cursor.description]

    return pd.DataFrame(results, columns=column_names)


def load_payee() -> pd.DataFrame:
    with sqlite3.connect(MMEX_PATH) as connection:
        cursor: sqlite3.Cursor = connection.cursor()
        results: list = cursor.execute(
            """
            SELECT
                PAYEEID, PAYEENAME
            FROM
                PAYEE_V1
            """
        ).fetchall()

        column_names: list[str] = [description[0] for description in cursor.description]

    return pd.DataFrame(results, columns=column_names)


def load_transfers(path: str) -> pd.DataFrame:
    transfers: pd.DataFrame = pd.read_csv(
        path,
        names=[
            "Data",
            "Stato",
            "Tipo",
            "Conto",
            "Beneficiario",
            "Importo",
            "Valuta",
            "Categoria",
            "Sotto-Categoria",
            "Note",
        ],
    )
    transfers["Beneficiario"] = transfers["Beneficiario"].str.replace("> ", "")
    return transfers


def save_account_trasnfers_to_db(account_path: pathlib.Path) -> None:
    tzinfo: pytz.timezone = pytz.timezone("Europe/Rome")
    followup_id, color = -1, -1
    transaction_number, deleted_time = "", ""
    status: str = "R"

    transfers_list: list[list[str | float | int | None | datetime]] = []

    transfers: pd.DataFrame = pd.read_csv(account_path)

    for _, row in transfers.iterrows():
        categ_id: str = (
            row.Categoria if pd.isna(row["Sotto-Categoria"]) else f"{row.Categoria}:{row['Sotto-Categoria']}"
        )
        categ_id: int = categories.loc[categ_id == categories.FULL_CATEGORY].CATEGID.to_numpy()[0]
        from_conto: int = accounts.loc[row.Conto == accounts.ACCOUNTNAME].ACCOUNTID.to_numpy()[0]
        transcode: str = row.Tipo

        if row.Tipo == "Transfer":
            print("a")

        try:
            to_conto: int = accounts.loc[row.ToConto == accounts.ACCOUNTNAME].ACCOUNTID.to_numpy()[0]
        except:
            to_conto: int = -1
        try:
            payee_id: int = payees.loc[row.Beneficiario == payees.PAYEENAME].PAYEEID.to_numpy()[0]
        except:
            payee_id: int = 2
        note = row.Note
        trans_date: str = row.Data
        amount: float = row.Importo
        update_time: str = datetime.now(tz=tzinfo).strftime(DATETIME_FORMAT)
        transaction_id: int = get_transaction_id(transactions_id)
        bisect.insort(a=transactions_id, x=transaction_id)
        transfers_list.append(
            [
                transaction_id,
                from_conto,
                to_conto,
                payee_id,
                transcode,
                amount,
                status,
                transaction_number,
                note,
                categ_id,
                trans_date,
                update_time,
                deleted_time,
                followup_id,
                amount,
                color,
            ]
        )

    mmex_transfers_df: pd.DataFrame = pd.DataFrame(transfers_list, columns=get_column_names_table("CHECKINGACCOUNT_V1"))

    # year: int = int(account_path.name.split(".")[0])
    mmex_transfers_df.to_csv(account_path.with_name(f"test_db_{account_path.name}"), index=False)
    print("data written to file")
    with sqlite3.connect(MMEX_PATH) as conn:
        mmex_transfers_df.to_sql("CHECKINGACCOUNT_V1", conn, if_exists="append", index=False)
        print("data written to db")


def get_transaction_id(transaction_ids: list[int]) -> int:
    return next(
        (idx for idx, trans_id in enumerate(transaction_ids) if idx != trans_id),
        -1,
    )


def preprocess_categories(categories: pd.DataFrame) -> pd.DataFrame:
    categories["FULL_CATEGORY"] = ""
    for idx, row in categories.sort_values("CATEGID").iterrows():
        full_category: list[str] = []
        parent_row: pd.Series = row.copy(deep=True)
        while True:
            if parent_row.PARENTID == -1:
                full_category.insert(0, parent_row.CATEGNAME)
                categories.loc[idx, "FULL_CATEGORY"] = (
                    ":".join(full_category) if len(full_category) > 1 else full_category[0]
                )
                break

            full_category.insert(0, parent_row.CATEGNAME)
            parent_row: pd.Series = categories[parent_row.PARENTID == categories.CATEGID].squeeze().copy(deep=True)

    return categories


if __name__ == "__main__":
    transactions_id: list[int] = get_transactions_id()
    accounts: pd.DataFrame = load_accounts()
    categories: pd.DataFrame = preprocess_categories(load_categories())
    payees: pd.DataFrame = load_payee()

    for account_path in sorted(pathlib.Path("/media/paolo/Kingston SSD/ledger-to-mmex/data").rglob("*[0-9]*.csv")):
        save_account_trasnfers_to_db(account_path)

    # try:
    #
    # except Exception as e:
    #     print(e)
    #     print(account_path)

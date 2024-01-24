#!/usr/bin/env python
# coding: utf-8

import dataclasses
import json
import os
import pathlib
import re

import pandas as pd

os.chdir(pathlib.Path.cwd().parents[0])

LEDGER_PATH: str = "~/Nextcloud/Note/Finanze/ledger/{}.csv"
OUTPUT_PATH: str = "data/{}.csv"
OUTPUT_COLUMNS: list[str] = [
    "Data",
    "Stato",
    "Tipo",
    "Conto",
    "ToConto",
    "Beneficiario",
    "Importo",
    "Valuta",
    "Categoria",
    "Sotto-Categoria",
    "Note",
]


@dataclasses.dataclass
class LedgerCols:
    DATE: str = "DATE"
    BOH: str = "BOH"
    DESCRIPTION: str = "DESCRIPTION"
    CATEGORY: str = "CATEGORY"
    CURRENCY: str = "CURRENCY"
    AMOUNT: str = "AMOUNT"
    BOH2: str = "BOH2"
    BOH3: str = "BOH3"


def detect_transaction_type(accounts: list[str]) -> str:
    for acc in [acc.split(":")[0].lower() for acc in accounts]:
        if acc == "guadagni":
            return "Deposit"
        if acc == "spese":
            return "Withdrawal"
    return "Transfer"


def extract_category(accounts: list[str], transaction_type: str) -> tuple[str, str]:
    if transaction_type == "Transfer":
        return "Trasferimento", "Trasferimento"

    for account in accounts:
        if "Assets" in account:
            continue

        splits: list[str] = mapped_categories[account].split(":")
        if len(splits) == 1:
            return splits[0], ""
        return ":".join(splits[:-1]), splits[-1]
    return "", ""


def extract_transaction_accounts_payee(transaction: pd.DataFrame) -> tuple[str, None, None]:
    from_account, to_account, payee = None, None, None

    for _, row in transaction.iterrows():
        if "Assets" in row[LedgerCols.CATEGORY] and not from_account:
            from_account = row[LedgerCols.CATEGORY].split(":")[-1]
    return from_account, to_account, payee


def extract_transfer_transaction_account_payee(transaction: pd.DataFrame) -> tuple[str, str, None]:
    from_account, to_account, payee = None, None, None

    for _, row in transaction.iterrows():
        if row[LedgerCols.AMOUNT] < 0 and not from_account:
            from_account = row[LedgerCols.CATEGORY].split(":")[-1]
        if row[LedgerCols.AMOUNT] > 0 and not to_account:
            to_account = row[LedgerCols.CATEGORY].split(":")[-1]

    return from_account, to_account, payee


def process_transaction(dataframe: pd.DataFrame):
    date: str = dataframe[LedgerCols.DATE][0].replace("/", "-")
    transaction_type: str = detect_transaction_type(dataframe[LedgerCols.CATEGORY].tolist())
    currency: str = "EUR"  # dataframe[LedgerCols.CURRENCY][0]
    category, sub_category = extract_category(dataframe[LedgerCols.CATEGORY].tolist(), transaction_type)
    amount: float = dataframe["AMOUNT_NORM"][0]
    note: str = dataframe[LedgerCols.DESCRIPTION][0]

    if transaction_type == "Transfer":
        from_account, to_account, payee = extract_transfer_transaction_account_payee(
            dataframe[[LedgerCols.CATEGORY, LedgerCols.AMOUNT]]
        )
    else:
        from_account, to_account, payee = extract_transaction_accounts_payee(dataframe[[LedgerCols.CATEGORY]])

    return [
        date,
        "R",
        transaction_type,
        conto_map.get(from_account, from_account),
        conto_map.get(to_account, to_account),
        payee,
        amount,
        currency,
        category,
        sub_category,
        note,
    ]


def convert_conto_name(old_conto: str | None) -> str | None:
    if not old_conto:
        return old_conto

    for k in conto_map:
        if k.lower() in old_conto.lower():
            return re.sub(k, conto_map[k], old_conto)

    return old_conto


with pathlib.Path("/data/mapped_categories.json").open("r") as f:
    mapped_categories: dict[str, str] = json.load(f)

conto_map: dict[str, str] = {"Intesa XME": "Intesa", "Contanti Sant'Arcangelo": "Casa"}

for year in (2021, 2022, 2023):
    ledger = pd.read_csv(
        LEDGER_PATH.format(year), header=None, names=[x.name for x in dataclasses.fields(LedgerCols())]
    ).drop(columns=[LedgerCols.BOH3, LedgerCols.BOH2, LedgerCols.BOH])
    ledger["AMOUNT_NORM"] = abs(ledger["AMOUNT"])
    ledger = ledger[ledger["DESCRIPTION"] != "Starting balances"]
    ledger = ledger.reset_index(drop=True)

    indexes: list[list[int]] = list(
        ledger.sort_values(by="DATE").groupby(["DATE", "DESCRIPTION", "AMOUNT_NORM"]).groups.values()
    )

    processed_transactions: list[list[str | float | None]] = [
        process_transaction(ledger.loc[idx].reset_index(drop=True)) for idx in indexes if len(idx) == 2
    ]

    processed_dataframe: pd.DataFrame = pd.DataFrame(processed_transactions, columns=OUTPUT_COLUMNS)
    processed_dataframe["Beneficiario"] = processed_dataframe["Beneficiario"].apply(convert_conto_name)
    processed_dataframe["Conto"] = processed_dataframe["Conto"].apply(convert_conto_name)
    processed_dataframe["Note"] = processed_dataframe["Note"].apply(convert_conto_name)

    processed_dataframe.to_csv(OUTPUT_PATH.format(year), index=False)

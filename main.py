import pathlib

from categories_extractor.ledger_categories_extractor import LedgerCategoriesExtractor
from categories_extractor.mmex_categories_extractor import MMEXCategoriesExtractor


def extract_ledger_categories(input_path: str, output_path: pathlib.Path) -> None:
    """Extract ledger categories from the input file and save them to the output file.

    Args:
        input_path (str): The path to the input file.
        output_path (pathlib.Path): The path to the output file.

    Returns:
        None

    Examples:
        >>> extract_ledger_categories("path/to/input.txt", pathlib.Path("path/to/output.txt"))
        # The ledger categories are extracted from the input file and saved to the output file.
    """
    ledger_categories_extractor: LedgerCategoriesExtractor = LedgerCategoriesExtractor(
        path=input_path, output_path=output_path
    )

    ledger_categories_extractor.execute()


def extract_mmex_categories(input_path: str, output_path: pathlib.Path) -> None:
    """Extract MMEX categories from the input file and save them to the output file.

    Args:
        input_path (str): The path to the input file.
        output_path (pathlib.Path): The path to the output file.

    Returns:
        None

    Examples:
        >>> extract_mmex_categories("path/to/input.txt", pathlib.Path("path/to/output.txt"))
        # The MMEX categories are extracted from the input file and saved to the output file.
    """
    mmex_categories_extractor: MMEXCategoriesExtractor = MMEXCategoriesExtractor(
        path=input_path, output_path=output_path
    )

    mmex_categories_extractor.execute()


if __name__ == "__main__":
    base_output_path: pathlib.Path = pathlib.Path.cwd() / "data"

    ledger_input_path: str = "~/Nextcloud/Note/Finanze/ledger/2022.csv"
    ledger_output_path: pathlib.Path = base_output_path / "ledger_categories.json"

    extract_ledger_categories(ledger_input_path, ledger_output_path)

    mmex_input_path: str = "/home/paolo/Nextcloud/MoneyManager/finances.mmb"
    mmex_output_path: pathlib.Path = base_output_path / "mmex_categories.json"

    extract_mmex_categories(mmex_input_path, mmex_output_path)

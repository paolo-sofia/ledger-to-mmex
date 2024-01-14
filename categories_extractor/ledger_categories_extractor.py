import dataclasses
import pathlib
from typing import Self

import pandas as pd

from categories_extractor.categories_extractor import CategoriesExtractor


@dataclasses.dataclass
class LedgerCols:
    """LedgerCols is a dataclass that represents the column names used in a ledger csv report.

    Attributes:
        DATE (str): The column name for the date.
        BOH (str): The column name for the beginning of the hour.
        DESCRIPTION (str): The column name for the description.
        CATEGORY (str): The column name for the category.
        CURRENCY (str): The column name for the currency.
        AMOUNT (str): The column name for the amount.
        BOH2 (str): The column name for the second beginning of the hour.
        BOH3 (str): The column name for the third beginning of the hour.
    """

    DATE: str = "DATE"
    BOH: str = "BOH"
    DESCRIPTION: str = "DESCRIPTION"
    CATEGORY: str = "CATEGORY"
    CURRENCY: str = "CURRENCY"
    AMOUNT: str = "AMOUNT"
    BOH2: str = "BOH2"
    BOH3: str = "BOH3"


class LedgerCategoriesExtractor(CategoriesExtractor):
    """LedgerCategoriesCombination extracts categories from a ledger csv report.

    Attributes:
        path (pathlib.Path): The path to the input file or directory.
        output_path (pathlib.Path): The path to the output file or directory.
        raw_categories (Iterator[T]): An iterator containing the raw categories to be parsed.
        categories (list[str]): a list containing the parsed categories

    Methods:
        __init__(self, path: str | pathlib.Path, output_path: str | pathlib.Path) -> None:
            Initialize a LedgerCategoriesCombination object with the provided path and output path.

        _parse_filepath(filepath: str | pathlib.Path) -> pathlib.Path:
            Parse a filepath and return it as a pathlib.Path object.

        read_data(self) -> Iterator[T]:
            Read data and save the results to the raw_category attribute.

        extract_categories(self, data: Iterator[T]) -> list[str]:
            Extract categories from data and save them to the categories attribute.

    """

    def __init__(self: Self, path: str | pathlib.Path, output_path: str | pathlib.Path) -> None:
        """Initialize a CategoriesExtractor object with the provided path and output path.

        Args:
            path (pathlib.Path | str): The path to the input file or directory.
            output_path (pathlib.Path | str): The path to the output file or directory.

        Raises:
            TypeError: If the path or output_path is not of type pathlib.Path or str.

        Returns:
            None

        Examples:
            >>> path_ = pathlib.Path("path/to/input.txt")
            >>> output_path_ = "path/to/output.txt"
            >>> CategoriesExtractor(path_, output_path_)
        """
        super().__init__(path=path, output_path=output_path)

    def read_data(self: Self) -> None:
        """Read data from a CSV file and extract unique categories.

        Returns:
            None
        """
        column_names: list[str] = [x.name for x in dataclasses.fields(LedgerCols())]
        self.raw_categories = (
            pd.read_csv(self.path, header=None, names=column_names)
            .drop(columns=[LedgerCols.BOH3, LedgerCols.BOH2, LedgerCols.BOH])[LedgerCols.CATEGORY]
            .drop_duplicates()
            .tolist()
        )

    def extract_categories(self: Self) -> None:
        """Extract categories from raw categories and store them in the categories attribute.

        Returns:
            None
        """
        all_combinations: list[str] = []

        self.raw_categories = [
            category
            for category in self.raw_categories
            if (not category.startswith("Assets")) and category != "StartingBalance"
        ]

        for category in self.raw_categories:
            split_category: list[str] = category.split(":")
            current_combinations = [":".join(split_category[: i + 1]) for i in range(len(split_category))]
            all_combinations.extend(current_combinations)

        self.categories = sorted(set(all_combinations))

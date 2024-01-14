import pathlib
import sqlite3
from typing import Self

import pandas as pd

from categories_extractor.categories_extractor import CategoriesExtractor


class MMEXCategoriesExtractor(CategoriesExtractor):
    """MMEXCategoriesExtractor extracts categories from a MMEX database.

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
        """Read categories from a MMEX SQLite database and return saves them to the raw_categories attribute."""
        connection: sqlite3.Connection = sqlite3.connect(self.path)
        cursor: sqlite3.Cursor = connection.cursor()
        results: list = cursor.execute(
            """
            SELECT
                *
            FROM
                CATEGORY_V1
            """
        ).fetchall()

        column_names: list[str] = [description[0] for description in cursor.description]
        connection.close()
        self.raw_categories: pd.DataFrame = pd.DataFrame(results, columns=column_names)

    def _find_all_combinations(
        self: Self,
        df: pd.DataFrame,
        parent_child_dict: dict[int, list[int]],
        current_id: int,
        current_combination: list[str],
    ) -> list[str]:
        """Find all combinations of categories in a DataFrame based on a parent-child dictionary.

        Args:
            df (pd.DataFrame): The DataFrame containing the categories.
            parent_child_dict (dict[int, list[int]]): A dictionary mapping parent category IDs to their child category
                IDs.
            current_id (int): The ID of the current category.
            current_combination (list[str]): The current combination of category names.

        Returns:
            list[str]: A list of all combinations of category names.
        """
        combinations: list[str] = []

        # Find all combinations using recursion
        if current_id not in parent_child_dict:
            return combinations

        for sub_id in parent_child_dict[current_id]:
            next_combination: list[str] = [*current_combination, df[df["CATEGID"] == sub_id]["CATEGNAME"].to_numpy()[0]]
            combinations.append(":".join(next_combination))
            combinations.extend(self._find_all_combinations(df, parent_child_dict, sub_id, next_combination))

        return combinations

    def _dataframe_to_combinations(self: Self) -> dict[str, list[str]]:
        """Convert the raw_categories DataFrame into a dictionary of combinations based on parent-child relationships.

        Returns:
            dict[str, list[str]]: A dictionary mapping each main category name to a list of all combinations of category
                names.
        """
        result_dict: dict[str, list[str]] = {}

        # Create a dictionary to store the parent-child relationships
        parent_child_dict: dict[int, list[int]] = {}

        # Iterate through the DataFrame to build the parent-child dictionary
        for _index, row in self.raw_categories.iterrows():
            parent_id: int = row["PARENTID"]
            current_id: int = row["CATEGID"]
            name: str = row["CATEGNAME"]

            # If parent_id is -1, it's a main name
            if parent_id == -1:
                result_dict[name] = []

            # Update the parent-child dictionary
            if parent_id in parent_child_dict:
                parent_child_dict[parent_id].append(current_id)
            else:
                parent_child_dict[parent_id] = [current_id]

        # Populate the result_dict with the parent-child relationships
        for main_name in result_dict:
            current_id: int = self.raw_categories.query("CATEGNAME == @main_name")["CATEGID"].to_numpy()[0]
            result_dict[main_name] = self._find_all_combinations(
                df=self.raw_categories,
                parent_child_dict=parent_child_dict,
                current_id=current_id,
                current_combination=[main_name],
            )

        return result_dict

    def _get_all_categories(self: Self, categories_combination: dict[str, list[str]]) -> None:
        """Get all categories from a dictionary of category combinations and saves them to the categories attribute.

        Args:
            categories_combination (dict[str, list[str]]): A dictionary mapping each main category name to a list of
                category combinations.
        """
        all_categories_combination: list[str] = []

        for main_name, combinations in categories_combination.items():
            all_categories_combination.extend([*combinations, main_name])
        self.categories = sorted(set(all_categories_combination))

    def extract_categories(self: Self) -> None:
        """Extract categories from the data and store them in the categories attribute.

        Returns:
            None

        Examples:
            This method is called internally and does not need to be invoked directly.
        """
        categories_dict: dict[str, list[str]] = self._dataframe_to_combinations()
        self._get_all_categories(categories_dict)

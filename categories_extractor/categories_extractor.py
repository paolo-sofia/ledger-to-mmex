import json
import pathlib
from abc import ABC, abstractmethod
from typing import Iterator, Self, TypeVar

T: TypeVar = TypeVar("T")


class FileExtensionError(Exception):
    """Exception raised when file extension is not the expected one."""

    def __init__(self: Self, message: str = "File extension is not the expected one.") -> None:
        """Initialize a custom exception with an optional error message.

        Args:
            message (str, optional): The error message. Defaults to "File extension is not the expected one."

        Examples:
            >>> raise FileExtensionError("Custom error message")
            FileExtensionError: Custom error message

            >>> raise FileExtensionError()
            FileExtensionError: File extension is not the expected one.
        """
        self.message = message
        super().__init__(self.message)


class CategoriesExtractor(ABC):
    """Abstract base class for extracting categories from data.

    Attributes:
        path (pathlib.Path): The path to the input file or directory.
        output_path (pathlib.Path): The path to the output file or directory.
        raw_categories (Iterator[T]): An iterator containing the raw categories to be parsed.
        categories (list[str]): a list containing the parsed categories

    Methods:
        __init__(self, path: str | pathlib.Path, output_path: str | pathlib.Path) -> None:
            Initialize a CategoriesExtractor object with the provided path and output path.

        _parse_filepath(filepath: str | pathlib.Path) -> pathlib.Path:
            Parse a filepath and return it as a pathlib.Path object.

        read_data(self) -> None:
            Read data and save the results to the raw_category attribute.

        extract_categories(self, data: Iterator[T]) -> None:
            Extract categories from data and save them to the categories attribute.

        save_categories_to_file(self) -> None:
            Save the categories attribute to the output file as json

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
            >>> CategoriesExtractor(path_, output_path_, None, None)
        """
        self.path: pathlib.Path = self._parse_filepath(path)
        self.output_path: pathlib.Path = self._parse_filepath(output_path)
        self.raw_categories: Iterator[T] | None = None
        self.categories: list[str] | None = None

    @staticmethod
    def _parse_filepath(filepath: str | pathlib.Path) -> pathlib.Path:
        """Parse a filepath and return it as a pathlib.Path object.

        Args:
            filepath (pathlib.Path | str): The filepath to parse.

        Returns:
            pathlib.Path: The parsed filepath as a pathlib.Path object.

        Raises:
            TypeError: If the filepath is not of type pathlib.Path or str.

        Examples:
            >>> file_path = pathlib.Path("path/to/file.txt")
            >>> CategoriesExtractor._parse_filepath(filepath)
            PosixPath('path/to/file.txt')

            >>> file_path = "path/to/file.txt"
            >>> CategoriesExtractor._parse_filepath(filepath)
            PosixPath('path/to/file.txt')
        """
        if isinstance(filepath, pathlib.Path):
            return filepath

        if isinstance(filepath, str):
            return pathlib.Path(filepath)

        raise TypeError("Invalid filepath type, must be pathlib.Path or str")

    def save_categories_to_file(self: Self) -> None:
        """Save the categories to a file in JSON format.

        Raises:
            ValueError: If the categories attribute is empty.

        Returns:
            None

        Examples:
            This method is called internally and does not need to be invoked directly.
        """
        if not self.categories:
            raise ValueError("Categories attribute is empty")

        if self.output_path.suffix[1:] != "json":
            raise FileExtensionError("Output file is not a json file")

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.output_path.exists():
            with self.output_path.open("r") as file:
                self.categories = list(set(self.categories + json.load(file)))

        with self.output_path.open("w") as f:
            json.dump(self.categories, f, indent=4)

    @abstractmethod
    def read_data(self: Self) -> None:
        """Read data and save the results to the raw_categories attribute.

        Examples:
            This is an abstract method and should be implemented in a subclass.
        """

    @abstractmethod
    def extract_categories(self: Self) -> None:
        """Extract categories from data and saves the list of categories into the categories attribute.

        Examples:
            This is an abstract method and should be implemented in a subclass.
        """

    def execute(self: Self) -> None:
        """Execute the steps to read data, extract categories, and save the data.

        Returns:
            None

        Examples:
            >>> extractor = CategoriesExtractor(...)
            >>> extractor.execute()
        """
        self.read_data()
        self.extract_categories()
        self.save_categories_to_file()

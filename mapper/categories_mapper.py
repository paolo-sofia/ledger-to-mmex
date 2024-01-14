import json
import pathlib
from typing import Self

import torch
from sentence_transformers import SentenceTransformer, util

# model: SentenceTransformer = SentenceTransformer("distiluse-base-multilingual-cased-v1")


class CategoriesMapper:
    def __init__(
        self: Self, ledger_file_path: str | pathlib.Path, mmex_file_path: str | pathlib.Path, model_name: str
    ) -> None:
        self.ledger_categories: list[str] = self._open_file(ledger_file_path)
        self.mmex_categories: list[str] = self._open_file(mmex_file_path)
        self.model: SentenceTransformer = SentenceTransformer(model_name)

    def _open_file(self: Self, path: str | pathlib.Path) -> list[str]:
        if not isinstance(path, (str, pathlib.Path)):
            raise TypeError("Invalid filepath type, must be pathlib.Path or str")

        if isinstance(path, str):
            path: pathlib.Path = pathlib.Path(path)

        with path.open("r") as file:
            return json.load(file)

    def map_ledger_to_mmex(self: Self) -> dict[str, str]:
        ledger_embeddings: torch.Tensor = self.model.encode(self.ledger_categories)
        mapped_categories: dict[str, str] = {}
        for category in self.mmex_categories:
            embedding: torch.Tensor = self.model.encode(category)
            most_similar_index: int = util.pytorch_cos_sim(embedding, ledger_embeddings).argmax().item()
            mapped_categories[self.ledger_categories[most_similar_index]] = category

        return mapped_categories


if __name__ == "__main__":
    ledger_file_path = pathlib.Path("/media/paolo/Kingston SSD/ledger-to-mmex/data/ledger_categories.json")
    mmex_file_path = pathlib.Path("/media/paolo/Kingston SSD/ledger-to-mmex/data/mmex_categories.json")
    model_name = "distiluse-base-multilingual-cased-v1"
    mapper = CategoriesMapper(ledger_file_path, mmex_file_path, model_name)
    mapper.map_ledger_to_mmex()

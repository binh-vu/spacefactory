from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from urllib.parse import urljoin

from drepr.main import OutputFormat
from duneflow.models import RawCell, RawTable
from duneflow.ops.curation import semantic_model_curator
from duneflow.ops.drepr import convert_data_from_sm, transformations
from duneflow.ops.formatter import to_column_based_table
from duneflow.ops.reader import TableScraperActor, TableScraperArgs
from duneflow.ops.select import select_table
from duneflow.ops.writer import write_table_to_file
from fins.dag import *
from libactor.dag import DAG, Flow, PartialFn
from libactor.storage import GlobalStorage
from sm.dataset import FullTable
from sm.inputs.table import ColumnBasedTable
from sm.namespaces.prelude import DefaultKnowledgeGraphNamespace
from tqdm import tqdm

cwd = Path(__file__).parent

GlobalStorage.init(cwd / "storage")


base_uri = "http://space.isi.edu/resource/"
urls = {
    "https://www.factoriesinspace.com/human-spaceflight": "Human Spaceflight & Landers",
    "https://www.factoriesinspace.com/cargo-transportation": "Cargo Transportation & Landers",
    "https://www.factoriesinspace.com/surface-spacecraft": "Surface Spacecraft",
    "https://www.factoriesinspace.com/space-stations": "Space Stations & Space Habitats",
    "https://www.factoriesinspace.com/surface-habitats": "Surface Habitats & Surface Structures",
    "https://www.factoriesinspace.com/in-space-manufacturing": "In-Space Manufacturing",
    "https://www.factoriesinspace.com/space-resources": "Space Resources",
    "https://www.factoriesinspace.com/space-utilities": "Space Utilities",
    "https://www.factoriesinspace.com/in-space-transportation": "In-Space Transportation",
    "https://www.factoriesinspace.com/miscellaneous": "Miscellaneous",
}


def split_cells(tbl_: IdentObj[ColumnBasedTable]) -> IdentObj[ColumnBasedTable]:
    tbl = deepcopy(tbl_.value)
    name2col = {c.clean_name: c for c in tbl.columns}
    for name in ["Category", "Service Type", "Field", "Status"]:
        name2col[name].values = [
            [x.strip() for x in v.split(",")] for v in name2col[name].values
        ]
    return IdentObj(tbl_.key + "_split", tbl)


def add_missing_product_name(tbl_: IdentObj[RawTable]) -> IdentObj[RawTable]:
    new_product_idx = tbl_.value.shape()[1]
    data = tbl_.value.add_column(new_product_idx, RawCell)
    nrows, ncols = data.shape()
    data[0, new_product_idx].value = "Product ID"
    for ri in range(1, nrows):
        if str(data[ri, 1].value).strip() == "":
            data[ri, 1].value = str(data[ri, 0].value).strip() + "'s Product"
        data[ri, new_product_idx].value = (
            str(data[ri, 0].value).strip() + " - " + str(data[ri, 1].value).strip()
        )
    return IdentObj(
        key=tbl_.key + "_missing_productname",
        value=tbl_.value.update_data(data.data),
    )


def make_link_cols(tbl_: IdentObj[RawTable]) -> IdentObj[RawTable]:
    data = tbl_.value.add_column(1, RawCell)
    nrows, ncols = data.shape()
    data.data[0][1].value = "URL"
    for row in data.data[1:]:
        metadata = row[0].metadata
        links = metadata.get("links", [])
        if len(links) > 0:
            row[1].value = max(links, key=lambda link: link["end"] - link["start"])[
                "url"
            ]
    return IdentObj(
        key=tbl_.key + "_link_cols",
        value=tbl_.value.update_data(data.data),
    )


def make_parent_category(
    tbl_: IdentObj[RawTable], tbl_category: str
) -> IdentObj[RawTable]:
    data = tbl_.value.add_column(2, RawCell)
    nrows, ncols = data.shape()
    data.data[0][2].value = "Parent Category"
    for row in data.data[1:]:
        row[2].value = tbl_category
    return IdentObj(
        key=tbl_.key + "_parent_category",
        value=tbl_.value.update_data(data.data),
    )


for url, category in tqdm(urls.items()):
    output_dir = cwd / "output" / url.split("/")[-1]
    output_dir.mkdir(parents=True, exist_ok=True)
    # output_dir = cwd / "output"

    dag = create_dag(
        {
            "table": [
                TableScraperActor(TableScraperArgs(max_num_hop=0)),
                PartialFn(select_table, idx=0),
                add_missing_product_name,
                make_link_cols,
                make_parent_category,
                PartialFn(
                    write_table_to_file,
                    outdir=output_dir,
                    format="csv",
                ),
                to_column_based_table,
                split_cells,
            ],
            "sem_model": Flow(
                "table",
                PartialFn(
                    semantic_model_curator, outdir=output_dir.parent, format="yml"
                ),
            ),
            "export": [
                Flow(
                    ["table", "sem_model"],
                    PartialFn(convert_data_from_sm, format=OutputFormat.TTL),
                ),
                PartialFn(write_ttl, outdir=output_dir, normalize=True),
            ],
        },
    )

    # (output_dir / ("description.yml")).write_text(
    #     (output_dir.parent / "description.yml")
    #     .read_text()
    #     .replace(
    #         "{{ CATEGORY }}",
    #         urljoin(base_uri, sha256(category.encode()).hexdigest()),
    #     )
    # )
    dag.process(
        {"table": (url,)},
        {"export"},
        {
            "sm": None,
            "kgns": kgns,
            "ontology": ontology,
            "base_uri": base_uri,
            "tbl_category": category,
        },
    )

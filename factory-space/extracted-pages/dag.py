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
urls = [
    "https://www.factoriesinspace.com/beyond-leo-remote-sensing",
    "https://www.factoriesinspace.com/geoengineering",
    "https://www.factoriesinspace.com/power-beaming",
    "https://www.factoriesinspace.com/propellant-refilling",
    "https://www.factoriesinspace.com/reentry-vehicles",
    "https://www.factoriesinspace.com/space-elevator",
    "https://www.factoriesinspace.com/space-entertainment",
    "https://www.factoriesinspace.com/space-solar-power",
    "https://www.factoriesinspace.com/space-tourism",
]


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


for url in tqdm(urls):
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

    dag.process(
        {"table": (url,)},
        {"export"},
        {
            "sm": None,
            "kgns": kgns,
            "ontology": ontology,
            "base_uri": base_uri,
        },
    )

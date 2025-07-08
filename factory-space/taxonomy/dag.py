from __future__ import annotations

import inspect
import textwrap
from copy import deepcopy
from inspect import getsource
from pathlib import Path

from drepr.main import OutputFormat
from drepr.models.prelude import DREPR_URI
from duneflow.ops.curation import (
    SemanticModelCuratorActor,
    SemanticModelCuratorArgs,
    semantic_model_curator,
)
from duneflow.ops.drepr import convert_data_from_sm
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
output_dir = cwd / "output"
GlobalStorage.init(cwd / "storage")


dag = create_dag(
    {
        "table": [
            TableScraperActor(TableScraperArgs(max_num_hop=0)),
            # PartialFn(select_table, idx=0),
            select_table,
            to_column_based_table,
            PartialFn(
                write_table_to_file,
                outdir=output_dir,
                format="csv",
            ),
        ],
        "sem_model": Flow(
            "table",
            # SemanticModelCuratorActor(SemanticModelCuratorArgs(output_dir, "yml")),
            PartialFn(semantic_model_curator, outdir=output_dir, format="yml"),
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


for i in [0, 2]:
    dag.process(
        {"table": ("https://www.factoriesinspace.com/graphs-taxonomy",)},
        {"export"},
        {
            "sm": None,
            "kgns": kgns,
            "ontology": ontology,
            "idx": i,
            "filename": f"taxonomy_{i}",
            "base_uri": "http://space.isi.edu/resource/",
        },
    )

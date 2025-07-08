from __future__ import annotations

import csv
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
from drepr.main import OutputFormat
from duneflow.models import RawCell, RawTable
from duneflow.ops.curation import semantic_model_curator
from duneflow.ops.drepr import convert_data_from_sm, transformations
from duneflow.ops.formatter import to_column_based_table
from duneflow.ops.reader import TableScraperActor, TableScraperArgs, get_html
from duneflow.ops.select import select_table
from duneflow.ops.writer import write_table_to_file
from libactor.dag import DAG, Flow, PartialFn
from libactor.storage import GlobalStorage
from rdflib import Literal, URIRef
from rsoup.core import Document
from sm.dataset import FullTable
from sm.inputs.table import ColumnBasedTable
from sm.namespaces.prelude import DefaultKnowledgeGraphNamespace
from tqdm import tqdm

from fins.dag import *

cwd = Path(__file__).parent

GlobalStorage.init(cwd / "storage")

base_uri = "http://space.isi.edu/resource/"


def extract_company(company: str, url: str) -> Graph:
    doc = Document(url, get_html(url).value)

    el = doc.select("#content > div:nth-child(2)")[0]
    infos = el.select(".row .dl-horizontal")
    founded = infos[0].select("dd")[0].get_text()
    assert infos[0].select("dt")[0].get_text() == "Founded"
    country = infos[0].select("dd")[1].get_text()

    g = Graph()
    g.add((URIRef(company), Space.founded, Literal(founded)))
    g.add((URIRef(company), Space.country, Literal(country)))

    for info in infos[1:]:
        if info.select("dt")[0].get_text() == "Funding":
            funding = info.select("dd")[0].get_text()
            g.add((URIRef(company), Space.funding, Literal(funding)))
        elif info.select("dt")[0].get_text() == "Website":
            website = info.select("dd")[0].get_text()
            g.add((URIRef(company), Space.website, Literal(website)))
        # else:
        #     raise ValueError(f"Unknown info {info.select('dt')[0].get_text()}")
    return g


# dag = create_dag(
#     {
#         "table": [extract_company],
#     },
# )

# Path to companies.csv file
companies_csv = cwd.parent / "companies.csv"

# Read companies from CSV file using pandas
df = pd.read_csv(companies_csv)
g = Graph()
for _, row in tqdm(list(df.iterrows())):
    g += extract_company(
        row["company"],
        row["url"],
    )

g.serialize(cwd / "companies.ttl", format="turtle")
g.serialize(cwd / "companies.ttl", format="turtle")

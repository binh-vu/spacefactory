from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from duneflow.ops.curation import SemanticModelCuratorActor, SemanticModelCuratorArgs
from duneflow.ops.reader import TableScraperActor, TableScraperArgs
from duneflow.ops.select import select_table
from duneflow.ops.writer import write_table_to_file, write_ttl
from kgdata.models import (
    MultiLingualString,
    MultiLingualStringList,
    Ontology,
    OntologyProperty,
)
from libactor.cache import IdentObj
from libactor.dag import DAG, Flow, PartialFn
from libactor.storage import GlobalStorage
from rdflib import OWL, RDF, RDFS, XSD, Graph, Namespace
from sm.dataset import FullTable
from sm.inputs.table import ColumnBasedTable
from sm.namespaces.prelude import DefaultKnowledgeGraphNamespace


class FinsNamespace(DefaultKnowledgeGraphNamespace):
    """Namespace for DBpedia entities and ontology"""

    entity_id: str = str(OWL.Thing)
    entity_uri: str = str(OWL.Thing)
    entity_label: str = "Thing"
    statement_uri: str = str(RDF.Statement)
    main_namespaces: list[str] = ["https://space.isi.edu/ontology/"]

    @classmethod
    def create(cls):
        return cls.from_prefix2ns(
            {
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "owl": "http://www.w3.org/2002/07/owl#",
                "space": "https://space.isi.edu/ontology/",
                "dc": "http://purl.org/dc/elements/1.1/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "drepr": "https://purl.org/drepr/1.0/",
            }
        )


Space = Namespace("https://space.isi.edu/ontology/")
DRepr = Namespace("https://purl.org/drepr/1.0/")
kgns = FinsNamespace.create()
props = {}
for puri in [
    RDFS.label,
    RDFS.comment,
    Space.url,
    Space.product,
    Space.category,
    Space.sub_category,
    Space.parent_category,
    Space.destination,
    Space.technology,
    Space.ism_material,
    Space.founded,
    Space.ism_first_launched,
    Space.field,
    Space.status,
    DRepr.blank,
    DRepr.uri,
]:
    puri = str(puri)
    prop = OntologyProperty(
        id=kgns.uri_to_id(puri),
        label=MultiLingualString.en(kgns.get_rel_uri(puri)),
        description=MultiLingualString.en(""),
        aliases=MultiLingualStringList.en([]),
        datatype=str(XSD.string),
        parents=[],
        related_properties=[],
        equivalent_properties=[],
        inverse_properties=[],
        instanceof=[str(RDF.Property)],
        ancestors={},
        domains=[],
        ranges=[],
    )
    props[prop.id] = prop
props[str(DRepr.uri)].datatype = str(DRepr.uri)
props[str(Space.url)].datatype = str(XSD.anyURI)

ontology = IdentObj("v100", Ontology("fins", kgns, {}, props))


def create_dag(dictmap: dict, type_conversions: Optional[list] = None):
    def to_column_based_table(tbl: FullTable) -> ColumnBasedTable:
        return tbl.table

    def to_column_based_table_v2(
        tbl: IdentObj[FullTable],
    ) -> IdentObj[ColumnBasedTable]:
        return IdentObj(tbl.key + "(colbase)", tbl.value.table)

    def to_column_based_table_v3(
        tbl: IdentObj[FullTable],
    ) -> ColumnBasedTable:
        return tbl.value.table

    return DAG.from_dictmap(
        dictmap,
        (type_conversions or [])
        + [
            to_column_based_table,
            to_column_based_table_v2,
            to_column_based_table_v3,
        ],
    )

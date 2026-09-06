from neo4j import GraphDatabase, Driver
import logging
import os
import time
from typing import Any, Iterable, Sequence
from src.builders.Neo4j.schema.nodes import *
from src.builders.Neo4j.schema.edges import *
from src.builders.batcher import batched
from src.builders.Neo4j.OntologyState import OntoStateManager

logger = logging.getLogger(__name__)

neo4j_username = os.getenv("NEO4J_USERNAME")
neo4j_password = os.getenv("NEO4J_PASSWORD")

if neo4j_username is None:
    raise ValueError("NEO4J_USERNAME is not set")
if neo4j_password is None:
    raise ValueError("NEO4J_PASSWORD is not set")


GRAPH_AUTH: tuple[str, str] = (neo4j_username, neo4j_password)



class Neo4j_ETL:
    def __init__(self):
        self._driver = None
        self._ontology_manager = None

    def _get_wsl_host_ip(self)->str:
        """
        Returns the windows host ip as seen from WSL2.
        Falls back to localhost if look up fails.
        """
        import subprocess
        try:
            result = subprocess.run(
                ["ip", "route", "show"],
                capture_output=True, text=True, check=True
            )
            for line in result.stdout.splitlines():
                if line.startswith("default"):
                    return line.split()[2]
        except (subprocess.CalledProcessError, IndexError, FileNotFoundError):
            pass
        return "localhost"

    @property
    def driver(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                f"bolt://{self._get_wsl_host_ip()}:7687",
                auth= GRAPH_AUTH
            )
            self._driver.verify_connectivity()
        return self._driver

    @property
    def ontology_manager(self):
        if self._ontology_manager is None:
            self._ontology_manager = OntoStateManager(self.driver)
        return self._ontology_manager

    def _run(self, tx, cypher, rows, batch_size= 2000):
        for batch in batched(rows, batch_size):
            tx.run(cypher, rows=batch)

    def _group_by_type_execute(self, neo4j_objects: Sequence[BaseNeo4jEdge|BaseNeo4jNode], action: Literal['upsert', 'delete']):
        neo4j_objects = list(neo4j_objects)
        if not neo4j_objects:
            return

        first = neo4j_objects[0]
        if isinstance(first, BaseNeo4jEdge):
            base = BaseNeo4jEdge
        elif isinstance(first, BaseNeo4jNode):
            base = BaseNeo4jNode
        else:
            raise TypeError(f"Unsupported neo4j object type: {type(first)}")

        assert self.driver is not None, ConnectionError("Couldn't connect to the database using Neo4j Driver")

        type_name = type(first).__name__
        logger.info("%s %s: %d row(s) to process", action, type_name, len(neo4j_objects))

        with self.driver.session() as session:
            for cypher, rows in base.batch_cypher(neo4j_objects, action):
                start = time.perf_counter()
                session.execute_write(self._run, cypher, rows)
                elapsed = time.perf_counter() - start
                logger.info(
                    "%s %s: wrote %d row(s) in %.2fs (%.0f rows/s)",
                    action, type_name, len(rows), elapsed, len(rows) / elapsed if elapsed > 0 else float("inf"),
                )

    

    
    def upsert_nodes(self, nodes: Sequence[BaseNeo4jNode]):
        self._group_by_type_execute(nodes, 'upsert')

    def delete_nodes(self, nodes: Sequence[BaseNeo4jNode]):
        self._group_by_type_execute(nodes, 'delete')

    def upsert_edges(self, edges: Sequence[BaseNeo4jEdge]):
        self._group_by_type_execute(edges, 'upsert')

    def delete_edges(self, edges: Sequence[BaseNeo4jEdge]):
        self._group_by_type_execute(edges, 'delete')
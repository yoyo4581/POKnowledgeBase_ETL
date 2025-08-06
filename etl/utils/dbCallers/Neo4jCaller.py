from neo4j import GraphDatabase
import os
from utils.colored_text import RED, GREEN, RESET, YELLOW


GRAPH_URI = os.getenv("GRAPH_URI")
GRAPH_AUTH = (os.getenv("GRAPH_USER"), os.getenv("GRAPH_PASSWORD"))

class Neo4jCaller:
    def __init__(self):
        try:
            with GraphDatabase.driver(GRAPH_URI, auth=GRAPH_AUTH) as self.graph_driver:
                self.graph_driver.verify_connectivity()
        except:
            print(f'{RED}Error connecting with Neo4j database. Check credentials and connection string.{RESET}')
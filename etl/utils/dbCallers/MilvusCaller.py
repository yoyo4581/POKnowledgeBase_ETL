from dotenv import load_dotenv
from utils.colored_text import RED, GREEN, RESET, YELLOW
from pymilvus import MilvusClient, DataType
import os

load_dotenv()


ZILLIS_URI = os.getenv("ZILLIS_URI")
ZILLIS_TOKEN = os.getenv("ZILLIS_TOKEN")

class MilvusCaller:
    def __init__(self):
        try:
            self.milvus_client = MilvusClient(uri=ZILLIS_URI, token=ZILLIS_TOKEN)
            print(f"{GREEN}Successfully connected to Zillis Milvus database!{RESET}")

            collection_name = "abstract_search"
            check_collection = self.milvus_client.has_collection(collection_name)
            if not check_collection:
                print(f"{RED}Collection '{collection_name}' does not exist in Zillis Milvus database.{RESET}")
            else:
                print(f"{GREEN}Collection '{collection_name}' exists in Zillis Milvus database.{RESET}")

        except Exception as e:
            print("Error connecting to Zillis Milvus database:", e)


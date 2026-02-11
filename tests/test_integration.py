import pytest
import pandas as pd
from pymongo import MongoClient
from scripts.local_transfert_csv_mongodb_P5_test import transfert_csv_mongodb_P5_test
from scripts.local_optimiser_collection_P5_test import optimiser_collection_P5_test
from scripts.local_test_integrite_donnees_P5_test import tester_integrite_donnees_P5_test

@pytest.fixture
def mongo_client():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["P5_test"]
    collection = db["dataset_donnees_medicales"]
    yield client
    collection.delete_many({})

def test_pipeline_complet(mongo_client):
    test_data = pd.DataFrame({'Name': ['John Doe'], 'Age': [30]})
    test_data.to_csv("test_data.csv", index=False)

    transfert_csv_mongodb_P5_test(csv_file_path="test_data.csv", db_name="P5_test", collection_name="dataset_donnees_medicales")

    db = mongo_client["P5_test"]
    collection = db["dataset_donnees_medicales"]
    assert collection.count_documents({}) > 0

    optimiser_collection_P5_test(mongo_uri="mongodb://localhost:27017/", db_name="P5_test", collection_name="dataset_donnees_medicales")

    tester_integrite_donnees_P5_test(csv_file_path="test_data.csv", db_name="P5_test", collection_name="dataset_donnees_medicales")

    
    assert collection.count_documents({}) == 1

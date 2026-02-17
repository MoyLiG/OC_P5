# conftest.py
import pytest
import sys
from pathlib import Path
from unittest.mock import patch
from pymongo import MongoClient
import pandas as pd
import os

# Ajoute le chemin du projet au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

@pytest.fixture
def test_collection():
    """Fixture pour obtenir une collection de test MongoDB"""
    # Configuration de test
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/")
    client = MongoClient(mongo_uri)
    db = client["test_db"]
    collection = db["test_collection"]

    # Nettoyage avant le test
    collection.delete_many({})

    yield collection

    # Nettoyage après le test
    collection.delete_many({})

@pytest.fixture
def sample_csv():
    """Fixture pour créer un DataFrame de test"""
    return pd.DataFrame({
        'Name': ['John Doe', 'Jane Smith'],
        'Age': [30, 25],
        'Medical Condition': ['Flu', 'Broken Arm']
    })

@pytest.fixture
def mock_env():
    """Fixture pour configurer les variables d'environnement"""
    with patch.dict(os.environ, {
        "CSV_FILE_PATH": "data/healthcare_dataset.csv",
        "DB_NAME": "test_db",
        "MONGO_URI": "mongodb://root:example@localhost:27017/"
    }):
        yield

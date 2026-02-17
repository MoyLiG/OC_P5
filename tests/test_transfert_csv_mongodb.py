"""
Test unitaire pour transfert_csv_mongodb.py
À exécuter depuis : P5/ (racine du projet)
Commande : pytest tests/test_transfert_csv_mongodb.py -v
"""
import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from scripts.transfert_csv_mongodb import transfert_csv_mongodb

@patch('scripts.transfert_csv_mongodb.os.path.exists')
@patch('scripts.transfert_csv_mongodb.MongoClient')
@patch('scripts.transfert_csv_mongodb.pd.read_csv')
def test_transfert_csv(mock_read_csv, mock_mongo_client, mock_path_exists):
    """
    Test de la fonction transfert_csv_mongodb avec mock complet de pandas et MongoDB.
    Scénario : environnement de test (DB_NAME = P5_test)
    → la collection est nettoyée avant l'insertion
    """
    # Mock des données CSV
    mock_read_csv.return_value = pd.DataFrame({
        'Name': ['John Doe'],
        'Age': [30]
    })

    # Simule que le fichier CSV existe
    mock_path_exists.return_value = True

    # Configuration de la hiérarchie MongoDB
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_client = MagicMock()

    # Configuration des mocks
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection

    # Simule une collection avec des données existantes
    mock_collection.count_documents.return_value = 5

    # list_collection_names retourne une liste de collections à nettoyer
    mock_db.list_collection_names.return_value = ["dataset_donnees_medicales"]

    # Configuration spécifique pour delete_many
    mock_delete_result = MagicMock()
    mock_delete_result.deleted_count = 5
    mock_collection.delete_many.return_value = mock_delete_result

    # Configuration du résultat d'insertion
    mock_insert_result = MagicMock()
    mock_insert_result.inserted_ids = [1]
    mock_collection.insert_many.return_value = mock_insert_result

    mock_mongo_client.return_value = mock_client

    # Configuration des variables d'environnement (base de test)
    with patch.dict(os.environ, {
        "CSV_FILE_PATH": "data/healthcare_dataset.csv",
        "DB_NAME": "P5_test",
        "MONGO_URI": "mongodb://root:example@mongodb:27017/"
    }):
        transfert_csv_mongodb()

    # Vérifications
    # 1. Le CSV a été lu avec le bon chemin
    mock_read_csv.assert_called_once_with("data/healthcare_dataset.csv")

    # 2. La collection a bien été nettoyée avant l'insertion
    mock_collection.delete_many.assert_called_once_with({})

    # 3. Les données ont été insérées après le nettoyage
    mock_collection.insert_many.assert_called_once()

    # 4. La connexion MongoDB a été fermée
    mock_client.close.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
"""
Test unitaire pour transfert_csv_mongodb_test_docker
À exécuter depuis : P5/ (racine du projet)
Commande : pytest tests/test_transfert_csv_mongodb_test_docker.py -v
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from scripts.transfert_csv_mongodb_test_docker import transfert_csv_mongodb_test_docker

@patch('scripts.transfert_csv_mongodb_test_docker.MongoClient')
@patch('scripts.transfert_csv_mongodb_test_docker.pd.read_csv')
def test_transfert_csv(mock_read_csv, mock_mongo_client):
    """
    Test de la fonction transfert_csv_mongodb_test_docker.
    Vérifie que les données CSV sont correctement transférées vers MongoDB.
    """
    # Mock des données CSV
    mock_read_csv.return_value = pd.DataFrame({
        'Name': ['John Doe'], 
        'Age': [30]
    })
    
    # Configuration de la hiérarchie MongoDB
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_client = MagicMock()
    
    # Structure : client -> db -> collection
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection
    mock_mongo_client.return_value = mock_client
    
    # Mock du résultat d'insertion
    mock_result = MagicMock()
    mock_result.inserted_ids = [1]
    mock_collection.insert_many.return_value = mock_result
    
    # Exécution de la fonction
    transfert_csv_mongodb_test_docker(csv_file_path="data/healthcare_dataset.csv")
    
    # Vérifications
    # 1. Le CSV a été lu avec le bon chemin
    mock_read_csv.assert_called_once_with("data/healthcare_dataset.csv")
    
    # 2. La collection a été vidée avant l'insertion
    mock_collection.delete_many.assert_called_once_with({})
    
    # 3. Les données ont été insérées
    mock_collection.insert_many.assert_called_once()
    
    # 4. La connexion MongoDB a été fermée
    mock_client.close.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
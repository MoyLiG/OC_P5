"""
Test unitaire pour transfert_csv_mongodb_test_docker
À exécuter depuis : P5/ (racine du projet)
Commande : pytest tests/test_transfert_csv_mongodb_test_docker.py -v
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import os
# On importe la fonction à tester
from scripts.transfert_csv_mongodb_test_docker import transfert_csv_mongodb_test_docker

@patch('scripts.transfert_csv_mongodb_test_docker.MongoClient')
@patch('scripts.transfert_csv_mongodb_test_docker.pd.read_csv')
def test_transfert_csv(mock_read_csv, mock_mongo_client):
    """
    Test mis à jour pour correspondre à la logique de production (Docker/CI).
    """
    # 1. Mock des données CSV
    mock_read_csv.return_value = pd.DataFrame({
        'Name': ['John Doe'], 
        'Age': [30]
    })
    
    # 2. Configuration du mock MongoDB
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_client = MagicMock()
    
    # On simule la hiérarchie client -> db -> collection
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection
    mock_mongo_client.return_value = mock_client
    
    # LOGIQUE IMPORTANTE : On simule que la collection est VIDE (0 documents)
    # pour permettre au script de continuer l'insertion
    mock_collection.count_documents.return_value = 0
    
    # 3. Exécution de la fonction
    # On utilise patch.dict pour simuler les variables d'environnement si nécessaire
    with patch.dict(os.environ, {"CSV_FILE_PATH": "data/healthcare_dataset.csv", "MONGO_URI": "mongodb://localhost:27017/"}):
        transfert_csv_mongodb_test_docker()
    
    # 4. Vérifications (Assertions)
    
    # Vérifie que le CSV a été lu (le chemin vient maintenant de l'env var)
    mock_read_csv.assert_called_once_with("data/healthcare_dataset.csv")
    
    # Vérifie que le script a bien vérifié si la collection était vide
    mock_collection.count_documents.assert_called_once_with({})
    
    # Vérifie que l'insertion a bien été tentée
    mock_collection.insert_many.assert_called_once()
    
    # Vérifie la fermeture de la connexion
    mock_client.close.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
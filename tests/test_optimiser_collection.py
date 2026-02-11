"""
Test unitaire pour optimiser_collection_P5_test
À placer dans : P5/tests/test_optimiser_collection.py
À exécuter depuis : P5/ (racine du projet)
Commande : pytest tests/test_optimiser_collection.py -v
"""
import pytest
from unittest.mock import patch, MagicMock, call
from scripts.local_optimiser_collection_P5_test import optimiser_collection_P5_test

@patch('scripts.local_optimiser_collection_P5_test.MongoClient')
def test_optimiser_collection(mock_mongo_client):
    """
    Test de la fonction optimiser_collection_P5_test.
    Vérifie que les index sont correctement créés sur la collection MongoDB.
    """
    # Création des mocks
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_client = MagicMock()
    
    # Configuration de la hiérarchie MongoDB : client -> db -> collection
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection
    mock_mongo_client.return_value = mock_client
    
    # Mock des méthodes de la collection
    mock_collection.find.return_value = []
    mock_collection.index_information.return_value = {}
    
    # Exécution de la fonction à tester
    optimiser_collection_P5_test()
    
    # Vérifications : les 4 index doivent être créés
    mock_collection.create_index.assert_any_call([("Name", 1)])
    mock_collection.create_index.assert_any_call([("Medical Condition", 1)])
    mock_collection.create_index.assert_any_call([("Date of Admission", 1)])
    mock_collection.create_index.assert_any_call([("Medical Condition", 1), ("Age", 1)])
    
    # Vérification du nombre exact d'appels
    assert mock_collection.create_index.call_count == 4, \
        f"Expected 4 index creations, got {mock_collection.create_index.call_count}"
    
    # Vérification que la connexion MongoDB est fermée
    mock_client.close.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
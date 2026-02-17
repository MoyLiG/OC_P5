"""
Test unitaire pour optimiser_collection
À exécuter depuis : P5/ (racine du projet)
Commande : pytest tests/test_optimiser_collection.py -v
"""
import pytest
from unittest.mock import patch, MagicMock
from scripts.optimiser_collection import optimiser_collection

@patch('scripts.optimiser_collection.MongoClient')
def test_optimiser_collection(mock_mongo_client):
    """
    Test de la fonction optimiser_collection.
    Vérifie que les index sont correctement créés sur la collection MongoDB.
    Scénario : environnement de test (DB_NAME = P5_test)
    """
    # Création des mocks
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_client = MagicMock()

    # Configuration de la hiérarchie MongoDB : client -> db -> collection
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection
    mock_mongo_client.return_value = mock_client

    # Simule une collection vide (pas de documents à normaliser)
    mock_collection.find.return_value = []
    mock_collection.index_information.return_value = {}

    # Exécution de la fonction à tester avec la base de test
    optimiser_collection(
        mongo_uri="mongodb://root:example@mongodb:27017/",
        db_name="P5_test"
    )

    # Vérification que les index existants ont été supprimés avant recréation
    mock_collection.drop_indexes.assert_called_once()

    # Vérifications : les 5 index doivent être créés avec leurs noms explicites
    mock_collection.create_index.assert_any_call([("Name", 1)], name="Name_1")
    mock_collection.create_index.assert_any_call([("Medical Condition", 1)], name="Medical_Condition_1")
    mock_collection.create_index.assert_any_call([("Date of Admission", 1)], name="Date_of_Admission_1")
    mock_collection.create_index.assert_any_call([("Medical Condition", 1), ("Age", 1)], name="Medical_Condition_1_Age_1")
    mock_collection.create_index.assert_any_call([("$**", "text")], name="text_index")

    # Vérification du nombre exact d'appels
    assert mock_collection.create_index.call_count == 5, \
        f"Expected 5 index creations, got {mock_collection.create_index.call_count}"

    # Vérification que la connexion MongoDB est fermée
    mock_client.close.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
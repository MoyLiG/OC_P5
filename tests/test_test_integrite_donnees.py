"""
Test unitaire pour test_integrite_donnees
À exécuter depuis : P5/ (racine du projet)
Commande : pytest tests/test_test_integrite_donnees.py -v
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from scripts.test_integrite_donnees import tester_integrite_donnees

@patch('scripts.test_integrite_donnees.os.path.exists')
@patch('scripts.test_integrite_donnees.MongoClient')
@patch('scripts.test_integrite_donnees.pd.read_csv')
def test_integrite_donnees(mock_read_csv, mock_mongo_client, mock_path_exists):
    """
    Test de la fonction tester_integrite_donnees.
    Vérifie que la fonction compare correctement les données CSV et MongoDB.
    Scénario : données CSV et MongoDB identiques → intégrité validée.
    """
    # Simule que le fichier CSV existe
    mock_path_exists.return_value = True

    # Configuration des données mockées (CSV et MongoDB identiques)
    mock_data = {
        'Name': ['John Doe', 'Jane Smith'],
        'Age': [30, 25],
        'Gender': ['Male', 'Female']
    }
    mock_read_csv.return_value = pd.DataFrame(mock_data)

    # Configuration de la hiérarchie MongoDB
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_client = MagicMock()

    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection
    mock_mongo_client.return_value = mock_client

    # IMPORTANT : _id est exclu car il n'existe pas dans le CSV
    # La fonction fait pd.DataFrame(list(collection.find()))
    # donc les clés des dicts deviennent les colonnes de df_mongo
    mock_collection.find.return_value = [
        {'Name': 'John Doe', 'Age': 30, 'Gender': 'Male'},
        {'Name': 'Jane Smith', 'Age': 25, 'Gender': 'Female'}
    ]
    mock_collection.count_documents.return_value = 2

    # Simule la présence de tous les index requis par la fonction
    mock_collection.index_information.return_value = {
        "_id_":                      {"key": [("_id", 1)]},
        "Name_1":                    {"key": [("Name", 1)]},
        "Medical_Condition_1":       {"key": [("Medical Condition", 1)]},
        "Date_of_Admission_1":       {"key": [("Date of Admission", 1)]},
        "Medical_Condition_1_Age_1": {"key": [("Medical Condition", 1), ("Age", 1)]}
    }

    # Exécution de la fonction avec la base de test
    result = tester_integrite_donnees(
        csv_file_path="data/healthcare_dataset.csv",
        mongo_uri="mongodb://root:example@mongodb:27017/",
        db_name="P5_test"
    )

    # Vérifications de base
    # 1. Le CSV a été lu avec le bon chemin
    mock_read_csv.assert_called_once_with("data/healthcare_dataset.csv")

    # 2. La collection MongoDB a été interrogée
    mock_collection.find.assert_called()

    # 3. Les index ont été vérifiés
    mock_collection.index_information.assert_called_once()

    # 4. La connexion a été fermée
    mock_client.close.assert_called_once()

    # 5. La fonction ne doit pas retourner False (index manquants)
    assert result is not False, "La fonction ne devrait pas retourner False si les index sont présents"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
"""
Test unitaire pour tester_integrite_donnees_P5_test
À placer dans : P5/tests/test_test_integrite_donnees.py
À exécuter depuis : P5/ (racine du projet)
Commande : pytest tests/test_test_integrite_donnees.py -v
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from scripts.local_test_integrite_donnees_P5_test import tester_integrite_donnees_P5_test

@patch('scripts.local_test_integrite_donnees_P5_test.MongoClient')
@patch('scripts.local_test_integrite_donnees_P5_test.pd.read_csv')
def test_integrite_donnees(mock_read_csv, mock_mongo_client):
    """
    Test de la fonction tester_integrite_donnees_P5_test.
    Vérifie que la fonction compare correctement les données CSV et MongoDB.
    """
    # Configuration des données mockées
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
    
    # Structure : client -> db -> collection
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection
    mock_mongo_client.return_value = mock_client
    
    # Configuration du retour de la méthode find
    # Important : doit correspondre aux données CSV pour que le test passe
    mock_collection.find.return_value = [
        {'Name': 'John Doe', 'Age': 30, 'Gender': 'Male'},
        {'Name': 'Jane Smith', 'Age': 25, 'Gender': 'Female'}
    ]
    
    # Exécution de la fonction
    # Note : La fonction devrait retourner True/False ou lever une exception
    # selon que les données sont intègres ou non
    result = tester_integrite_donnees_P5_test(csv_file_path="data/healthcare_dataset.csv")
    
    # Vérifications de base
    # 1. Le CSV a été lu
    mock_read_csv.assert_called_once_with("data/healthcare_dataset.csv")
    
    # 2. La collection MongoDB a été interrogée
    mock_collection.find.assert_called()
    
    # 3. La connexion a été fermée
    mock_client.close.assert_called_once()
    
    # Vérifications d'intégrité (si la fonction ne fait pas déjà ces vérifications)
    # Ces assertions vérifient que nos mocks sont cohérents
    csv_df = mock_read_csv.return_value
    mongo_df = pd.DataFrame(list(mock_collection.find.return_value))
    
    # Vérification des colonnes
    colonnes_csv = set(csv_df.columns)
    colonnes_mongo = set(mongo_df.columns)
    assert colonnes_csv == colonnes_mongo, \
        f"Colonnes différentes: CSV={colonnes_csv}, MongoDB={colonnes_mongo}"
    
    # Vérification du nombre de lignes
    assert len(csv_df) == len(mongo_df), \
        f"Nombre de lignes différent: CSV={len(csv_df)}, MongoDB={len(mongo_df)}"
    
    # Vérification des doublons
    doublons_csv = csv_df.duplicated().sum()
    doublons_mongo = mongo_df.duplicated().sum()
    assert doublons_csv == doublons_mongo, \
        f"Nombre de doublons différent: CSV={doublons_csv}, MongoDB={doublons_mongo}"
    
    # Vérification des valeurs manquantes par colonne
    valeurs_manquantes_csv = csv_df.isnull().sum()
    valeurs_manquantes_mongo = mongo_df.isnull().sum()
    pd.testing.assert_series_equal(
        valeurs_manquantes_csv, 
        valeurs_manquantes_mongo,
        check_names=False
    )

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
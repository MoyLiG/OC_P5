import os
import sys
import pytest
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Ajout du chemin pour les imports si nécessaire
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.gestion_utilisateurs import (
    creer_utilisateur,
    authentifier_utilisateur
)

# Configuration via variables d'environnement
MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "P5_test")
MONGO_ADMIN_USER = os.getenv("MONGO_ADMIN_USER", "admin_p5_test")

@pytest.fixture(scope="session")
def mongo_client():
    """Fixture pour obtenir un client MongoDB unique pour la session"""
    client = MongoClient(MONGO_URI)
    yield client
    client.close()

@pytest.fixture
def db_test(mongo_client):
    """Fixture pour obtenir l'objet Database (évite le conflit de nom avec la fonction)"""
    return mongo_client[DB_NAME]

@pytest.fixture(autouse=True)
def setup_test_data(db_test):
    """Nettoyage automatique avant et après chaque test"""
    # Nettoyage préventif
    db_test.users.delete_many({"username": {"$in": ["test_user_valid", MONGO_ADMIN_USER]}})
    
    yield db_test
    
    # Nettoyage final (correction du \$in erroné)
    db_test.users.delete_many({"username": {"$in": ["test_user_valid", MONGO_ADMIN_USER]}})

class TestRBACIntegration:
    """Classe de tests d'intégration pour le système RBAC"""

    def test_connexion_mongodb(self, mongo_client):
        """Vérifie que la connexion à la base de données est opérationnelle"""
        try:
            mongo_client.admin.command('ping')
            assert True
        except ConnectionFailure:
            pytest.fail("La connexion à MongoDB a échoué.")

    def test_creation_utilisateur_succes(self, db_test):
        """
        Teste la création d'un utilisateur avec des paramètres valides.
        Utilise l'injection de l'objet 'db' pour éviter les reconnexions.
        """
        # Données respectant les règles de gestion_utilisateurs.py
        username = "test_user_valid"
        password = "ComplexPassword123!" # > 12 car, Maj, Min, Chiffre, Spécial
        role = "lecteur" # Rôle présent dans le dictionnaire ROLES

        # Appel de la fonction avec l'objet db injecté
        success = creer_utilisateur(
            username=username,
            password=password,
            role=role,
            email="test@example.com",
            db=db_test
        )

        assert success is True

        # Vérification directe en base via la fixture
        user_in_db = db_test.users.find_one({"username": username})
        assert user_in_db is not None
        assert user_in_db["role"] == role
        assert "password_hash" in user_in_db

    def test_authentification_utilisateur(self, db_test):
        """Teste le cycle complet : création puis authentification"""
        username = "test_user_auth"
        password = "AuthPassword123!"
        role = "medecin"

        # 1. Création préalable
        creer_utilisateur(
            username=username,
            password=password,
            role=role,
            db=db_test
        )

        # 2. Tentative d'authentification
        user_session = authentifier_utilisateur(
            username=username,
            password=password,
            db_name=DB_NAME,
            mongo_uri=MONGO_URI
        )

        assert user_session is not None
        assert user_session["username"] == username
        assert "password_hash" not in user_session  # Sécurité : le hash ne doit pas être retourné

    def test_creation_utilisateur_role_invalide(self, db_test):
        """Vérifie que la création échoue si le rôle n'existe pas"""
        success = creer_utilisateur(
            username="invalid_role_user",
            password="ValidPassword123!",
            role="superman", # Rôle inexistant
            db=db_test
        )
        assert success is False
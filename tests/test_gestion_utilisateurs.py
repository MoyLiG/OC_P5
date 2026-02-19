"""
Tests unitaires pour le module gestion_utilisateurs
À exécuter depuis : P5/ (racine du projet)
Commande : pytest tests/test_gestion_utilisateurs.py -v
"""
import pytest
from unittest.mock import patch, MagicMock
from scripts.gestion_utilisateurs import (
    valider_mot_de_passe,
    hasher_mot_de_passe,
    verifier_mot_de_passe,
    creer_utilisateur,
    authentifier_utilisateur,
    ROLES
)

class TestValidationMotDePasse:
    """Tests de validation des mots de passe"""
    
    def test_mot_de_passe_trop_court(self):
        """Mot de passe < 12 caractères doit être rejeté"""
        valide, message = valider_mot_de_passe("Court1!")
        assert valide is False
        assert "12 caractères" in message
    
    def test_mot_de_passe_sans_majuscule(self):
        """Mot de passe sans majuscule doit être rejeté"""
        valide, message = valider_mot_de_passe("minuscule123!@#")
        assert valide is False
        assert "majuscule" in message
    
    def test_mot_de_passe_sans_minuscule(self):
        """Mot de passe sans minuscule doit être rejeté"""
        valide, message = valider_mot_de_passe("MAJUSCULE123!@#")
        assert valide is False
        assert "minuscule" in message
    
    def test_mot_de_passe_sans_chiffre(self):
        """Mot de passe sans chiffre doit être rejeté"""
        valide, message = valider_mot_de_passe("SansChiffre!@#Abc")
        assert valide is False
        assert "chiffre" in message
    
    def test_mot_de_passe_sans_caractere_special(self):
        """Mot de passe sans caractère spécial doit être rejeté"""
        valide, message = valider_mot_de_passe("SansSpecial123Abc")
        assert valide is False
        assert "caractère spécial" in message
    
    def test_mot_de_passe_valide(self):
        """Mot de passe respectant tous les critères"""
        valide, message = valider_mot_de_passe("Secure123!@#Pass")
        assert valide is True
        assert message == "Mot de passe valide"

class TestHashageMotDePasse:
    """Tests du hashage bcrypt"""
    
    def test_hash_genere_different_a_chaque_fois(self):
        """Deux hash du même mot de passe doivent être différents (salt aléatoire)"""
        password = "TestPassword123!@#"
        hash1 = hasher_mot_de_passe(password)
        hash2 = hasher_mot_de_passe(password)
        assert hash1 != hash2
    
    def test_verification_mot_de_passe_correct(self):
        """Vérifier qu'un mot de passe correct est accepté"""
        password = "TestPassword123!@#"
        hashed = hasher_mot_de_passe(password)
        assert verifier_mot_de_passe(password, hashed) is True
    
    def test_verification_mot_de_passe_incorrect(self):
        """Vérifier qu'un mauvais mot de passe est rejeté"""
        password = "TestPassword123!@#"
        hashed = hasher_mot_de_passe(password)
        assert verifier_mot_de_passe("MauvaisMotDePasse!", hashed) is False
    
    def test_hash_commence_par_bcrypt_prefix(self):
        """Le hash bcrypt doit commencer par $2b$"""
        password = "TestPassword123!@#"
        hashed = hasher_mot_de_passe(password)
        assert hashed.startswith("$2b$")

@patch('scripts.gestion_utilisateurs.MongoClient')
class TestCreationUtilisateur:
    """Tests de création d'utilisateurs"""
    
    def test_creation_utilisateur_valide(self, mock_mongo_client):
        """Créer un utilisateur avec des paramètres valides"""
        # Configuration des mocks
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_client = MagicMock()
        
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        # Simule qu'aucun utilisateur n'existe
        mock_collection.find_one.return_value = None
        
        # Création de l'utilisateur
        result = creer_utilisateur(
            username="test_user",
            password="ValidPassword123!@#",
            role="medecin",
            email="test@hospital.fr",
            mongo_uri="mongodb://localhost:27017/",
            db_name="P5_test"
        )
        
        assert result is True
        mock_collection.insert_one.assert_called_once()
        
        # Vérifier que le document inséré contient les bonnes clés
        call_args = mock_collection.insert_one.call_args[0][0]
        assert call_args["username"] == "test_user"
        assert call_args["role"] == "medecin"
        assert "password_hash" in call_args
        assert call_args["password_hash"].startswith("$2b$")
    
    def test_creation_utilisateur_existant(self, mock_mongo_client):
        """Ne pas créer un utilisateur qui existe déjà"""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_client = MagicMock()
        
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        # Simule qu'un utilisateur existe déjà
        mock_collection.find_one.return_value = {"username": "test_user"}
        
        result = creer_utilisateur(
            username="test_user",
            password="ValidPassword123!@#",
            role="medecin",
            mongo_uri="mongodb://localhost:27017/",
            db_name="P5_test"
        )
        
        assert result is False
        mock_collection.insert_one.assert_not_called()
    
    def test_creation_utilisateur_role_invalide(self, mock_mongo_client):
        """Rejeter un rôle invalide"""
        result = creer_utilisateur(
            username="test_user",
            password="ValidPassword123!@#",
            role="role_inexistant",
            mongo_uri="mongodb://localhost:27017/",
            db_name="P5_test"
        )
        
        assert result is False
    
    def test_creation_utilisateur_mot_de_passe_faible(self, mock_mongo_client):
        """Rejeter un mot de passe faible"""
        result = creer_utilisateur(
            username="test_user",
            password="faible",
            role="medecin",
            mongo_uri="mongodb://localhost:27017/",
            db_name="P5_test"
        )
        
        assert result is False

@patch('scripts.gestion_utilisateurs.MongoClient')
class TestAuthentification:
    """Tests d'authentification"""
    
    def test_authentification_reussie(self, mock_mongo_client):
        """Authentifier un utilisateur avec le bon mot de passe"""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_client = MagicMock()
        
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        # Hash du mot de passe "TestPassword123!@#"
        password = "TestPassword123!@#"
        hashed = hasher_mot_de_passe(password)
        
        # Simule un utilisateur existant
        mock_collection.find_one.return_value = {
            "_id": "123",
            "username": "test_user",
            "password_hash": hashed,
            "role": "medecin",
            "active": True
        }
        
        result = authentifier_utilisateur(
            username="test_user",
            password=password,
            mongo_uri="mongodb://localhost:27017/",
            db_name="P5_test"
        )
        
        assert result is not None
        assert result["username"] == "test_user"
        assert "password_hash" not in result  # Ne doit pas être retourné
        
        # Vérifier que la date de dernière connexion a été mise à jour
        mock_collection.update_one.assert_called_once()
    
    def test_authentification_mot_de_passe_incorrect(self, mock_mongo_client):
        """Rejeter un mauvais mot de passe"""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_client = MagicMock()
        
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        password = "TestPassword123!@#"
        hashed = hasher_mot_de_passe(password)
        
        mock_collection.find_one.return_value = {
            "_id": "123",
            "username": "test_user",
            "password_hash": hashed,
            "role": "medecin",
            "active": True
        }
        
        result = authentifier_utilisateur(
            username="test_user",
            password="MauvaisMotDePasse!",
            mongo_uri="mongodb://localhost:27017/",
            db_name="P5_test"
        )
        
        assert result is None
    
    def test_authentification_utilisateur_inexistant(self, mock_mongo_client):
        """Rejeter un utilisateur qui n'existe pas"""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_client = MagicMock()
        
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        mock_collection.find_one.return_value = None
        
        result = authentifier_utilisateur(
            username="utilisateur_inexistant",
            password="MotDePasse123!@#",
            mongo_uri="mongodb://localhost:27017/",
            db_name="P5_test"
        )
        
        assert result is None
    
    def test_authentification_compte_desactive(self, mock_mongo_client):
        """Rejeter un compte désactivé"""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_client = MagicMock()
        
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client.return_value = mock_client
        
        password = "TestPassword123!@#"
        hashed = hasher_mot_de_passe(password)
        
        mock_collection.find_one.return_value = {
            "_id": "123",
            "username": "test_user",
            "password_hash": hashed,
            "role": "medecin",
            "active": False  # Compte désactivé
        }
        
        result = authentifier_utilisateur(
            username="test_user",
            password=password,
            mongo_uri="mongodb://localhost:27017/",
            db_name="P5_test"
        )
        
        assert result is None

class TestRoles:
    """Tests des rôles et permissions"""
    
    def test_roles_definis(self):
        """Vérifier que tous les rôles attendus sont définis"""
        roles_attendus = ["admin", "medecin", "infirmier", "lecteur"]
        for role in roles_attendus:
            assert role in ROLES
    
    def test_roles_ont_permissions(self):
        """Chaque rôle doit avoir des permissions"""
        for role, config in ROLES.items():
            assert "permissions" in config
            assert len(config["permissions"]) > 0
    
    def test_admin_a_toutes_permissions(self):
        """L'admin doit avoir la permission manage_users"""
        assert "manage_users" in ROLES["admin"]["permissions"]

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

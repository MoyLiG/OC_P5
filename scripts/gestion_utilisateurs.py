"""
Gestion des utilisateurs applicatifs avec hashage sécurisé des mots de passe
Utilise bcrypt pour le hashing (résistant aux attaques GPU)
"""
from pymongo import MongoClient
import bcrypt
import os
from dotenv import load_dotenv
from datetime import datetime
import re
from datetime import datetime, timezone

# Détermine l'environnement
env = os.getenv("ENVIRONMENT", "local")

# Charge le fichier .env approprié
if env == "local":
    load_dotenv(".env.local")
elif env == "test":
    load_dotenv(".env.test")
elif env == "docker":
    load_dotenv(".env.docker")
elif env == "test_docker":
    load_dotenv(".env.test_docker")
else:
    load_dotenv()

# Rôles applicatifs disponibles
ROLES = {
    "admin": {
        "description": "Administrateur système - tous les droits",
        "permissions": ["read", "write", "delete", "manage_users"]
    },
    "medecin": {
        "description": "Médecin - lecture et écriture des dossiers médicaux",
        "permissions": ["read", "write"]
    },
    "infirmier": {
        "description": "Infirmier - lecture et écriture limitée",
        "permissions": ["read", "write_limited"]
    },
    "lecteur": {
        "description": "Lecteur - consultation uniquement",
        "permissions": ["read"]
    }
}

def valider_mot_de_passe(password):
    """
    Valide la robustesse du mot de passe selon les standards de sécurité
    - Au moins 12 caractères
    - Au moins 1 majuscule, 1 minuscule, 1 chiffre, 1 caractère spécial
    """
    if len(password) < 12:
        return False, "Le mot de passe doit contenir au moins 12 caractères"
    
    if not re.search(r"[A-Z]", password):
        return False, "Le mot de passe doit contenir au moins une majuscule"
    
    if not re.search(r"[a-z]", password):
        return False, "Le mot de passe doit contenir au moins une minuscule"
    
    if not re.search(r"\d", password):
        return False, "Le mot de passe doit contenir au moins un chiffre"
    
    if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", password):
        return False, "Le mot de passe doit contenir au moins un caractère spécial"
    
    return True, "Mot de passe valide"

def hasher_mot_de_passe(password):
    """
    Hash un mot de passe avec bcrypt
    Utilise un salt automatique et 12 rounds (équilibre sécurité/performance)
    """
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')  # Stocké en string dans MongoDB

def verifier_mot_de_passe(password, hashed_password):
    """
    Vérifie qu'un mot de passe correspond à son hash
    """
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))

# ... (garder les imports et ROLES identiques)

def creer_utilisateur(username, password, role, email=None, db=None, mongo_uri=None, db_name=None):
    """
    Crée un utilisateur. Accepte un objet 'db' existant pour l'injection de dépendance.
    """
    try:
        # 1. Validation du rôle
        if role not in ROLES:
            print(f"❌ Rôle invalide. Rôles disponibles : {list(ROLES.keys())}")
            return False
        
        # 2. Validation du mot de passe
        valide, message = valider_mot_de_passe(password)
        if not valide:
            print(f"❌ {message}")
            return False
        
        # 3. Gestion de la connexion (Injection ou nouvelle)
        _client = None
        if db is None:
            mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/")
            db_name = db_name or os.getenv("DB_NAME", "P5")
            _client = MongoClient(mongo_uri)
            db = _client[db_name]
        
        users_collection = db["users"]
        
        # 4. Vérification existence
        if users_collection.find_one({"username": username}):
            print(f"❌ L'utilisateur '{username}' existe déjà")
            if _client: _client.close()
            return False
        
        # 5. Création
        user_doc = {
            "username": username,
            "password_hash": hasher_mot_de_passe(password),
            "role": role,
            "permissions": ROLES[role]["permissions"],
            "email": email,
            "created_at": datetime.now(timezone.utc),
            "active": True
        }
        
        users_collection.insert_one(user_doc)
        users_collection.create_index("username", unique=True)
        
        print(f"✅ Utilisateur '{username}' créé.")
        if _client: _client.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False
        
        # Hash du mot de passe
        hashed_password = hasher_mot_de_passe(password)
        
        # Création du document utilisateur
        user_doc = {
            "username": username,
            "password_hash": hashed_password,
            "role": role,
            "permissions": ROLES[role]["permissions"],
            "email": email,
            "created_at": datetime.utcnow(),
            "last_login": None,
            "active": True
        }
        
        # Insertion dans MongoDB
        result = users_collection.insert_one(user_doc)
        
        # Création d'index unique sur username
        users_collection.create_index("username", unique=True)
        
        print(f"✅ Utilisateur '{username}' créé avec succès")
        print(f"   Rôle: {role} ({ROLES[role]['description']})")
        print(f"   Permissions: {', '.join(ROLES[role]['permissions'])}")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de la création de l'utilisateur : {e}")
        return False

def authentifier_utilisateur(username, password, mongo_uri=None, db_name=None):
    """
    Authentifie un utilisateur
    
    Args:
        username (str): Nom d'utilisateur
        password (str): Mot de passe en clair
        mongo_uri (str): URI MongoDB
        db_name (str): Nom de la base de données
    
    Returns:
        dict or None: Document utilisateur si authentification réussie, None sinon
    """
    try:
        mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/")
        db_name = db_name or os.getenv("DB_NAME", "P5")
        
        client = MongoClient(mongo_uri)
        db = client[db_name]
        users_collection = db["users"]
        
        # Récupérer l'utilisateur
        user = users_collection.find_one({"username": username})
        
        if not user:
            print(f"❌ Utilisateur '{username}' introuvable")
            client.close()
            return None
        
        if not user.get("active", True):
            print(f"❌ Compte '{username}' désactivé")
            client.close()
            return None
        
        # Vérification du mot de passe
        if verifier_mot_de_passe(password, user["password_hash"]):
            # Mise à jour de la date de dernière connexion
            users_collection.update_one(
                {"_id": user["_id"]},
                {"$set": {"last_login": datetime.now(timezone.utc)}}
            )
            
            print(f"✅ Authentification réussie pour '{username}'")
            client.close()
            
            # Retourne l'utilisateur sans le hash du mot de passe
            user.pop("password_hash", None)
            return user
        else:
            print(f"❌ Mot de passe incorrect pour '{username}'")
            client.close()
            return None
            
    except Exception as e:
        print(f"❌ Erreur lors de l'authentification : {e}")
        return None

def lister_utilisateurs(mongo_uri=None, db_name=None):
    """
    Liste tous les utilisateurs
    """
    try:
        mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/")
        db_name = db_name or os.getenv("DB_NAME", "P5")
        
        client = MongoClient(mongo_uri)
        db = client[db_name]
        users_collection = db["users"]
        
        users = list(users_collection.find({}, {"password_hash": 0}))
        
        if not users:
            print("Aucun utilisateur trouvé")
            return []
        
        print(f"\n📋 Liste des utilisateurs ({len(users)}):")
        print("-" * 80)
        for user in users:
            status = "✅ Actif" if user.get("active", True) else "❌ Désactivé"
            last_login = user.get("last_login", "Jamais connecté")
            if isinstance(last_login, datetime):
                last_login = last_login.strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"👤 {user['username']:20} | Rôle: {user['role']:15} | {status} | Dernière connexion: {last_login}")
        
        client.close()
        return users
        
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des utilisateurs : {e}")
        return []

def supprimer_utilisateur(username, mongo_uri=None, db_name=None):
    """
    Supprime un utilisateur (soft delete - désactivation)
    """
    try:
        mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/")
        db_name = db_name or os.getenv("DB_NAME", "P5")
        
        client = MongoClient(mongo_uri)
        db = client[db_name]
        users_collection = db["users"]
        
        result = users_collection.update_one(
            {"username": username},
            {"$set": {"active": False, "deactivated_at": datetime.utcnow()}}
        )
        
        if result.modified_count > 0:
            print(f"✅ Utilisateur '{username}' désactivé avec succès")
            client.close()
            return True
        else:
            print(f"❌ Utilisateur '{username}' introuvable")
            client.close()
            return False
            
    except Exception as e:
        print(f"❌ Erreur lors de la suppression : {e}")
        return False

if __name__ == "__main__":
    print("=== Gestion des utilisateurs applicatifs ===\n")
    
    # Exemple d'utilisation
    print("Création d'utilisateurs de test...")
    
    # Admin
    creer_utilisateur(
        username="admin",
        password="Admin123!@#Secure",
        role="admin",
        email="admin@hospital.fr"
    )
    
    # Médecin
    creer_utilisateur(
        username="dr.martin",
        password="Doctor456!@#Safe",
        role="medecin",
        email="dr.martin@hospital.fr"
    )
    
    # Lecteur
    creer_utilisateur(
        username="lecteur_test",
        password="Read789!@#Only",
        role="lecteur",
        email="lecteur@hospital.fr"
    )
    
    print("\n" + "="*80 + "\n")
    
    # Liste les utilisateurs
    lister_utilisateurs()
    
    print("\n" + "="*80 + "\n")
    
    # Test d'authentification
    print("Test d'authentification...")
    authentifier_utilisateur("admin", "Admin123!@#Secure")
    authentifier_utilisateur("admin", "mauvais_mot_de_passe")

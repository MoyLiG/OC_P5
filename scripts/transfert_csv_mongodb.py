import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pathlib import Path

# Détermine l'environnement
env = os.getenv("ENVIRONMENT", "local")  # 'local' par défaut

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
    # Charge le fichier par défaut si aucun environnement spécifique
    load_dotenv()

def transfert_csv_mongodb(csv_file_path=None, mongo_uri=None, db_name=None, collection_name="dataset_donnees_medicales"):
    try:
        # Utilisation des variables d'environnement ou des valeurs par défaut
        mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:example@mongodb:27017/")
        db_name = db_name or os.getenv("DB_NAME", "P5")  # Valeur par défaut pour la production
        csv_path = csv_file_path or os.getenv("CSV_FILE_PATH", "/data/healthcare_dataset.csv")

        print(f"Exécution en mode {env}")
        print(f"Base de données: {db_name}")
        print(f"MongoDB URI: {mongo_uri}")

        # Vérification de l'existence du fichier CSV
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Le fichier n'existe pas au chemin : {csv_path}")

        print(f"Fichier CSV trouvé: {csv_path}")

        df = pd.read_csv(csv_path)

        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Nettoyage de la base seulement en environnement de test
        is_test_env = db_name == "P5_test"
        if is_test_env:
            print(f"Environnement de test détecté. Nettoyage de la base '{db_name}'...")
            # Suppression de toutes les collections sauf 'system.*' (systèmes)
            for collection_name in db.list_collection_names():
                if not collection_name.startswith('system.'):
                    db[collection_name].delete_many({})
                    print(f"Collection '{collection_name}' vidée.")
        elif collection.count_documents({}) > 0:
            print(f"La collection '{collection_name}' contient déjà des données. Annulation de l'insertion pour éviter les doublons.")
            return

        data = df.to_dict(orient='records')
        collection.insert_many(data)
        print(f"{len(data)} documents insérés dans la collection '{collection_name}' de la base '{db_name}'.")

    except Exception as e:
        print(f"Une erreur est survenue : {e}")
        raise  # Crucial pour faire échouer le test GitHub Actions en cas d'erreur
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    print("Début de l'exécution de transfert_csv_mongodb.py")
    transfert_csv_mongodb()
    print("Fin de l'exécution de transfert_csv_mongodb.py")

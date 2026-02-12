import os
import pandas as pd
from pymongo import MongoClient

def transfert_csv_mongodb_test_docker():
    # 1. Configuration via variables d'environnement (avec valeurs par défaut pour Docker/Prod)
    # Si la variable n'existe pas, on prend la valeur de production
    csv_file_path = os.getenv("CSV_FILE_PATH", "/data/healthcare_dataset.csv")
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:example@mongodb:27017/")
    
    db_name = "P5_test_docker"
    collection_name = "dataset_donnees_medicales"

    try:
        # Lecture du fichier
        print(f"Chargement du fichier : {csv_file_path}")
        df = pd.read_csv(csv_file_path)

        # Connexion MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Logique d'évitement des doublons
        if collection.count_documents({}) > 0:
            print(f"La collection '{collection_name}' contient déjà des données. Annulation de l'insertion.")
            return

        # Insertion des données
        data = df.to_dict(orient='records')
        collection.insert_many(data)
        print(f"{len(data)} documents insérés avec succès dans {db_name}.")

    except Exception as e:
        print(f"Erreur lors de l'exécution : {e}")
        raise e # Crucial pour faire échouer le test GitHub Actions en cas d'erreur
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    print("--- Démarrage du script de migration ---")
    transfert_csv_mongodb_test_docker()
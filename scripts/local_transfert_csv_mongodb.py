import pandas as pd
from pymongo import MongoClient

def transfert_csv_mongodb(csv_file_path="data/healthcare_dataset.csv", mongo_uri="mongodb://localhost:27017/", db_name="P5", collection_name="dataset_donnees_medicales"):
    try:
        df = pd.read_csv(csv_file_path)

        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        if collection.count_documents({}) > 0:
            print(f"La collection '{collection_name}' contient déjà des données. Annulation de l'insertion pour éviter les doublons.")
            return

        data = df.to_dict(orient='records')
        collection.insert_many(data)
        print(f"{len(data)} documents insérés dans la collection '{collection_name}' de la base '{db_name}'.")

    except Exception as e:
        print(f"Une erreur est survenue : {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    print("Début de l'exécution de local_transfert_csv_mongodb.py")
    transfert_csv_mongodb()
    print("Fin de l'exécution de local_transfert_csv_mongodb.py")

import pandas as pd
from pymongo import MongoClient

def transfert_csv_mongodb_P5_test(csv_file_path="data/healthcare_dataset.csv", mongo_uri="mongodb://localhost:27017/", db_name="P5_test", collection_name="dataset_donnees_medicales"):
    try:
        # Lire le fichier CSV
        df = pd.read_csv(csv_file_path)
        print(f"Fichier {csv_file_path} lu avec succès. Nombre de lignes : {len(df)}")

        # Connexion à MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Vider la collection avant d'insérer de nouvelles données (optionnel, pour éviter les doublons)
        collection.delete_many({})
        print(f"Collection '{collection_name}' vidée.")

        # Conversion du DataFrame en une liste de dictionnaires
        data = df.to_dict(orient='records')

        # Insertion des données dans MongoDB
        result = collection.insert_many(data)
        print(f"{len(result.inserted_ids)} documents insérés dans la collection '{collection_name}' de la base '{db_name}'.")

    except FileNotFoundError:
        print(f"Erreur : Le fichier {csv_file_path} est introuvable.")
    except Exception as e:
        print(f"Une erreur est survenue : {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    print("Début de l'exécution de local_transfert_csv_mongodb_P5_test.py")
    transfert_csv_mongodb_P5_test()
    print("Fin de l'exécution de local_transfert_csv_mongodb_P5_test.py")

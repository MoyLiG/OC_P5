from pymongo import MongoClient, UpdateOne
import os
from datetime import datetime
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

def format_name(name):
    if not isinstance(name, str):
        return name
    return name.capitalize()

def optimiser_collection(mongo_uri=None, db_name=None, collection_name="dataset_donnees_medicales"):
    """
    Optimise la collection MongoDB en créant des index et en normalisant les données.
    Args:
        mongo_uri (str): URI de connexion à MongoDB
        db_name (str): Nom de la base de données
        collection_name (str): Nom de la collection
    """
    try:
        # Utilisation des variables d'environnement ou des valeurs par défaut
        mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:example@mongodb:27017/")
        db_name = db_name or os.getenv("DB_NAME", "P5")  # Valeur par défaut pour la production

        print(f"Exécution en mode {env}")
        print(f"Base de données: {db_name}")
        print(f"MongoDB URI: {mongo_uri}")

        # Connexion à MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        print(f"Optimisation de la collection '{collection_name}' dans la base '{db_name}'...")

        # Suppression des index existants pour éviter les conflits
        print("Suppression des index existants...")
        collection.drop_indexes()

        # Normalisation des noms et prénoms avec bulk_write
        print("Normalisation des noms et prénoms...")
        name_updates = []
        for document in collection.find({"Name": {"$exists": True}}):
            full_name = document["Name"]
            name_parts = full_name.split()
            formatted_name_parts = [format_name(part) for part in name_parts]
            formatted_name = " ".join(formatted_name_parts)
            name_updates.append(
                UpdateOne(
                    {"_id": document["_id"]},
                    {"$set": {"Name": formatted_name}}
                )
            )

        if name_updates:
            result = collection.bulk_write(name_updates)
            print(f"Mise à jour des noms et prénoms terminée. {result.modified_count} documents mis à jour.")

        # Conversion des dates d'admission avec bulk_write
        print("Conversion des dates d'admission...")
        admission_date_updates = []
        for document in collection.find({"Date of Admission": {"$type": "string"}}):
            try:
                date_str = document["Date of Admission"]
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                admission_date_updates.append(
                    UpdateOne(
                        {"_id": document["_id"]},
                        {"$set": {"Date of Admission": date_obj}}
                    )
                )
            except Exception as e:
                print(f"Erreur lors de la conversion de la date d'admission pour le document {document['_id']}: {e}")

        if admission_date_updates:
            result = collection.bulk_write(admission_date_updates)
            print(f"Dates d'admission corrigées : {result.modified_count}")

        # Conversion des dates de sortie avec bulk_write
        print("Conversion des dates de sortie...")
        discharge_date_updates = []
        for document in collection.find({"Discharge Date": {"$type": "string"}}):
            try:
                date_str = document["Discharge Date"]
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                discharge_date_updates.append(
                    UpdateOne(
                        {"_id": document["_id"]},
                        {"$set": {"Discharge Date": date_obj}}
                    )
                )
            except Exception as e:
                print(f"Erreur lors de la conversion de la date de sortie pour le document {document['_id']}: {e}")

        if discharge_date_updates:
            result = collection.bulk_write(discharge_date_updates)
            print(f"Dates de sortie corrigées : {result.modified_count}")

        # Création des index optimisés avec des noms explicites
        print("Création des index...")
        collection.create_index([("Name", 1)], name="Name_1")
        print("Index 'Name_1' créé sur le champ 'Name'")

        collection.create_index([("Medical Condition", 1)], name="Medical_Condition_1")
        print("Index 'Medical_Condition_1' créé sur le champ 'Medical Condition'")

        collection.create_index([("Date of Admission", 1)], name="Date_of_Admission_1")
        print("Index 'Date_of_Admission_1' créé sur le champ 'Date of Admission'")

        collection.create_index([("Medical Condition", 1), ("Age", 1)], name="Medical_Condition_1_Age_1")
        print("Index 'Medical_Condition_1_Age_1' créé sur ('Medical Condition', 'Age')")

        # Création d'un index texte pour la recherche dans les descriptions
        collection.create_index([("$**", "text")], name="text_index")
        print("Index 'text_index' créé sur tous les champs")

        # Affichage des index créés
        indexes = collection.index_information()
        print("\nIndex disponibles dans la collection :")
        for index_name, index_info in indexes.items():
            print(f"- {index_name}: {index_info['key']}")

        print("\nOptimisation de la collection terminée avec succès.")

    except Exception as e:
        print(f"Erreur lors de l'optimisation de la collection : {e}")
        raise  # Crucial pour faire échouer le test GitHub Actions en cas d'erreur
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    print("Début de l'exécution de optimiser_collection.py")
    optimiser_collection()
    print("Fin de l'exécution de optimiser_collection.py")

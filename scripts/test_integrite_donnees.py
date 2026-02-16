from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

# Détermine l'environnement
env = os.getenv("ENVIRONMENT", "docker")

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

def tester_integrite_donnees(csv_file_path=None, mongo_uri=None, db_name=None, collection_name="dataset_donnees_medicales"):

    """
    Teste l'intégrité des données entre un fichier CSV et une collection MongoDB.
    Args:
        csv_file_path (str): Chemin vers le fichier CSV
        mongo_uri (str): URI de connexion à MongoDB
        db_name (str): Nom de la base de données
        collection_name (str): Nom de la collection
    """
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
        
        df_csv = pd.read_csv(csv_path)

        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        df_mongo = pd.DataFrame(list(collection.find()))

        colonnes_csv = set(df_csv.columns)
        colonnes_mongo = set(df_mongo.columns)

        print("=== Vérification des colonnes ===")
        if colonnes_csv == colonnes_mongo:
            print("Les colonnes sont identiques entre le CSV et MongoDB.")
        else:
            print(f"Différence dans les colonnes : {colonnes_csv - colonnes_mongo}")

        print("\n=== Vérification des types de données ===")
        types_comparaison = pd.DataFrame({"CSV": df_csv.dtypes, "MongoDB": df_mongo.dtypes})
        print(types_comparaison)

        print("\n=== Vérification des doublons ===")
        doublons_csv = df_csv.duplicated().sum()
        doublons_mongo = df_mongo.duplicated().sum()
        print(f"Nombre de doublons dans le CSV : {doublons_csv}")
        print(f"Nombre de doublons dans MongoDB : {doublons_mongo}")

        print("\n=== Vérification des valeurs manquantes ===")
        valeurs_manquantes_csv = df_csv.isnull().sum()
        valeurs_manquantes_mongo = df_mongo.isnull().sum()
        print("Valeurs manquantes dans le CSV :\n", valeurs_manquantes_csv)
        print("\nValeurs manquantes dans MongoDB :\n", valeurs_manquantes_mongo)

        print("\n=== Vérification du nombre de lignes ===")
        print(f"Nombre de lignes dans le CSV : {len(df_csv)}")
        print(f"Nombre de lignes dans MongoDB : {len(df_mongo)}")

        print("\n=== Vérification des index ===")
        indexes = collection.index_information()
        required_indexes = ["Name_1", "Medical_Condition_1", "Date_of_Admission_1", "Medical_Condition_1_Age_1"]
        missing_indexes = [index for index in required_indexes if index not in indexes]
        if missing_indexes:
            print(f"Erreur : Les index suivants sont manquants : {missing_indexes}")
            return False
        else:
            print("Succès : Tous les index requis ont été créés.")
            

    except Exception as e:
        print(f"Une erreur est survenue : {e}")
        raise  # Crucial pour faire échouer le test GitHub Actions en cas d'erreur
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    print("Début de l'exécution de test_integrite_donnees.py")
    tester_integrite_donnees()
    print("Fin de l'exécution de test_integrite_donnees.py")

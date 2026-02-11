from pymongo import MongoClient
import pandas as pd

def tester_integrite_donnees(csv_file_path="data/healthcare_dataset.csv", mongo_uri="mongodb://localhost:27017/", db_name="P5", collection_name="dataset_donnees_medicales"):
    try:
        df_csv = pd.read_csv(csv_file_path)
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
        else:
            print("Succès : Tous les index requis ont été créés.")

    except Exception as e:
        print(f"Une erreur est survenue : {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    print("Début de l'exécution de local_test_integrite_donnees.py")
    tester_integrite_donnees()
    print("Fin de l'exécution de local_test_integrite_donnees.py")

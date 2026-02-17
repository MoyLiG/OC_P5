from pymongo import MongoClient
import os
from dotenv import load_dotenv
import pandas as pd

# Détermine l'environnement
env = os.getenv("ENVIRONMENT", "local")  # 'local' par défaut

# Charge le fichier .env approprié
if env == "local":
    load_dotenv(".env.local")
elif env == "test":
    load_dotenv(".env.test")
elif env == "docker":
    load_dotenv(".env.docker")
else:
    # Charge le fichier par défaut si aucun environnement spécifique
    load_dotenv()

def main():
    try:
        # Utilisation des variables d'environnement ou des valeurs par défaut
        mongo_uri = os.getenv("MONGO_URI", "mongodb://root:example@localhost:27017/")
        db_name = os.getenv("DB_NAME", "P5")  # Valeur par défaut
        csv_path = os.getenv("CSV_FILE_PATH", "healthcare_dataset.csv")

        print(f"Exécution en mode {env}")
        print(f"Base de données: {db_name}")
        print(f"MongoDB URI: {mongo_uri}")
        print(f"Chemin du CSV: {csv_path}")

        # Connexion à MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db["dataset_donnees_medicales"]

        print("\n=== Début des opérations CRUD ===")

        # 1. Create : Insérer des documents depuis le CSV
        df = pd.read_csv(csv_path)
        documents = df.to_dict(orient="records")
        collection.insert_many(documents)
        print(f"{len(documents)} documents insérés.")

        # 2. Create : Ajouter un nouveau patient
        nouveau_patient = {
            "Name": "Morgan Le Gall",
            "Age": 35,
            "Gender": "Male",
            "Blood Type": "O+",
            "Medical Condition": "Tendonitis",
            "Date of Admission": "2026-01-20",
            "Doctor": "Dr. Martin",
            "Hospital": "Hôpital Central",
            "Insurance Provider": "Assurance Santé",
            "Billing Amount": 1500.00,
            "Room Number": 101,
            "Admission Type": "Urgent",
            "Discharge Date": "2026-01-25",
            "Medication": "Anti-inflammatoires",
            "Test Results": "Normal"
        }
        collection.insert_one(nouveau_patient)
        print("Nouveau patient ajouté.")

        # 3. Read : Lire un patient spécifique
        patient = collection.find_one({"Name": "Morgan Le Gall"})
        print("\nPatient trouvé :")
        print(patient)

        # 4. Read : Lire tous les patients avec une condition médicale spécifique
        patients_diabetes = collection.find({"Medical Condition": "Diabetes"})
        print("\nPatients atteints de diabète :")
        for patient in patients_diabetes:
            print(patient["Name"], "-", patient["Medical Condition"])

        # 5. Read : Utiliser .isin pour filtrer les patients par groupe sanguin
        groupes_sanguins_a_filtrer = ["B-", "O+", "AB+"]
        patients_filtres = collection.find({"Blood Type": {"$in": groupes_sanguins_a_filtrer}})
        print("\nPatients avec groupe sanguin B-, O+ ou AB+ :")
        for patient in patients_filtres:
            print(patient["Name"], "-", patient["Blood Type"])

        # 6. Update : Mettre à jour la condition médicale d'un patient
        collection.update_one(
            {"Name": "Morgan Le Gall"},
            {"$set": {"Medical Condition": "Tendonitis (en traitement)"}}
        )
        print("\nCondition médicale mise à jour pour Morgan Le Gall.")

        # 7. Read : Vérifier la mise à jour
        patient_maj = collection.find_one({"Name": "Morgan Le Gall"})
        print("\nCondition médicale après mise à jour :", patient_maj["Medical Condition"])

        # 8. Delete : Supprimer un patient
        collection.delete_one({"Name": "Morgan Le Gall"})
        print("\nPatient Morgan Le Gall supprimé.")

        # 9. Read : Lire tous les patients admis en "Urgent"
        patients_urgents = collection.find({"Admission Type": "Urgent"})
        print("\nPatients admis en urgence :")
        for patient in patients_urgents:
            print(patient["Name"], "-", patient["Admission Type"])

        print("\n=== Opérations CRUD terminées ===")

    except Exception as e:
        print(f"Erreur lors de l'exécution: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()

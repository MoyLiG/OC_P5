from pymongo import MongoClient
import pandas as pd

# Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["P5"]
collection = db["dataset_donnees_medicales"]

# 1. Create : Insérer des documents depuis le CSV
df = pd.read_csv("healthcare_dataset.csv")
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

# 4. Read : Lire tous les patients avec une condition médicale spécifique (ex: "Diabetes")
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

# Fermer la connexion
client.close()

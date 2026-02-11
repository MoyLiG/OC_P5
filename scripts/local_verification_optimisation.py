from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["P5"]
collection = db["dataset_donnees_medicales"]

# 1. Vérifier et corriger le typage des dates
for document in collection.find({"Date of Admission": {"$type": "string"}}):
    date_str = document["Date of Admission"]
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    collection.update_one(
        {"_id": document["_id"]},
        {"$set": {"Date of Admission": date_obj}}
    )

for document in collection.find({"Discharge Date": {"$type": "string"}}):
    date_str = document["Discharge Date"]
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    collection.update_one(
        {"_id": document["_id"]},
        {"$set": {"Discharge Date": date_obj}}
    )

# 2. Créer des index pertinents
collection.create_index([("Name", 1)])
collection.create_index([("Medical Condition", 1)])
collection.create_index([("Date of Admission", 1)])
collection.create_index([("Medical Condition", 1), ("Age", 1)])

# 3. Lister les index créés
print("\nIndex créés dans la collection :")
for index_name, index_info in collection.index_information().items():
    print(f"{index_name}: {index_info}")

# Fermer la connexion
client.close()

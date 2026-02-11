from pymongo import MongoClient
import pandas as pd

# Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["P5"]
collection = db["dataset_donnees_medicales"]

# Exporter uniquement les patients avec un groupe sanguin spécifique
df = pd.DataFrame(list(collection.find()))
groupes_sanguins = ["O+", "B-", "A+"]
df_filtre = df[df["Blood Type"].isin(groupes_sanguins)]
df_filtre.to_csv("export_donnees_medicales_filtrees.csv", index=False)

# Fermer la connexion
client.close()
print("Export des données filtrées terminé.")

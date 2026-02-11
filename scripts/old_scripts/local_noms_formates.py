from pymongo import MongoClient

# Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["P5"]
collection = db["dataset_donnees_medicales"]

def format_name(name):
    """Formate un nom ou prénom : 1ère lettre en majuscule, le reste en minuscules."""
    if not isinstance(name, str):
        return name
    return name.capitalize()

# Mettre à jour tous les documents pour formater le champ "Name"
for document in collection.find():
    if "Name" in document:
        full_name = document["Name"]
        # Diviser en mots (pour gérer les noms composés)
        name_parts = full_name.split()
        formatted_name_parts = [format_name(part) for part in name_parts]
        formatted_name = " ".join(formatted_name_parts)

        # Mettre à jour le document
        collection.update_one(
            {"_id": document["_id"]},
            {"$set": {"Name": formatted_name}}
        )

print("Mise à jour des noms et prénoms terminée.")

# Fermer la connexion
client.close()

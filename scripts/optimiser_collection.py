from pymongo import MongoClient
from datetime import datetime

def format_name(name):
    if not isinstance(name, str):
        return name
    return name.capitalize()

def optimiser_collection(mongo_uri="mongodb://root:example@mongodb:27017/", db_name="P5", collection_name="dataset_donnees_medicales"):
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        count = 0
        for document in collection.find():
            if "Name" in document:
                full_name = document["Name"]
                name_parts = full_name.split()
                formatted_name_parts = [format_name(part) for part in name_parts]
                formatted_name = " ".join(formatted_name_parts)
                collection.update_one({"_id": document["_id"]}, {"$set": {"Name": formatted_name}})
                count += 1
        print(f"Mise à jour des noms et prénoms terminée. {count} documents mis à jour.")

        admission_dates_updated = 0
        for document in collection.find({"Date of Admission": {"$type": "string"}}):
            try:
                date_str = document["Date of Admission"]
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                collection.update_one({"_id": document["_id"]}, {"$set": {"Date of Admission": date_obj}})
                admission_dates_updated += 1
            except Exception as e:
                print(f"Erreur lors de la conversion de la date d'admission pour le document {document['_id']}: {e}")

        discharge_dates_updated = 0
        for document in collection.find({"Discharge Date": {"$type": "string"}}):
            try:
                date_str = document["Discharge Date"]
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                collection.update_one({"_id": document["_id"]}, {"$set": {"Discharge Date": date_obj}})
                discharge_dates_updated += 1
            except Exception as e:
                print(f"Erreur lors de la conversion de la date de sortie pour le document {document['_id']}: {e}")

        print(f"Dates d'admission corrigées : {admission_dates_updated}")
        print(f"Dates de sortie corrigées : {discharge_dates_updated}")

        print("\nCréation des index...")
        collection.create_index([("Name", 1)])
        collection.create_index([("Medical Condition", 1)])
        collection.create_index([("Date of Admission", 1)])
        collection.create_index([("Medical Condition", 1), ("Age", 1)])

        print("\nIndex créés dans la collection :")
        for index_name, index_info in collection.index_information().items():
            print(f"{index_name}: {index_info['key']}")

    except Exception as e:
        print(f"Une erreur est survenue : {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    print("Début de l'exécution de optimiser_collection.py")
    optimiser_collection()
    print("Fin de l'exécution de optimiser_collection.py")

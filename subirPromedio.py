import os
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()

# Conexión a MongoDB
client = MongoClient(os.getenv("MONGO_URI"))
MONGO_DB = os.getenv("MONGO_DB")
db = client[MONGO_DB]
collection = db["Employee"]
def subir_promedio_mongo(nombre: str, promedio: float):
    """
    Guarda o actualiza el promedio de un alumno en MongoDB usando solo su nombre.
    """
    if not nombre or promedio is None:
        return ValueError("nombre y promedio son obligatorios")

    collection.update_one(
        {"Nombre completo": nombre},
        {"$set": {"Promedio": promedio}},
        upsert=True  # crea si no existe
    )

    return True

if __name__ == "__main__":
    # Ejemplo de uso
    subir_promedio_mongo("Cristopher Esau Sanchez Vargas", 8.5)
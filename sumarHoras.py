import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from nombres import extraer_nombres_empleados

# 🔹 Cargar variables de entorno
load_dotenv()

# 🔐 Leer variables
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

# Validación básica
if not all([MONGO_URI, MONGO_DB, MONGO_COLLECTION]):
    raise ValueError("❌ Faltan variables de entorno de MongoDB")

# 🔹 Conexión MongoDB (SOLO LECTURA)
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

def asociar_empleados():
    empleado_df = extraer_nombres_empleados()
    empleado_df["name"] = empleado_df["name"].str.strip().str.lower()

    # 🔹 Leer MongoDB
    documentos = list(
        collection.find(
            {},
            {"_id": 0, "name": 1, "area": 1, "equipo": 1}
        )
    )

    base_de_datos = pd.DataFrame(documentos)

    if not base_de_datos.empty:
        base_de_datos["name"] = base_de_datos["name"].str.strip().str.lower()
    else:
        base_de_datos = pd.DataFrame(columns=["name", "area", "equipo"])

    combinado = pd.merge(empleado_df, base_de_datos, on="name", how="left")
    combinado["area"] = combinado["area"].fillna("Sin área")
    combinado["equipo"] = combinado["equipo"].fillna("Sin equipo")

    resultado = {}

    for name, group in combinado.groupby("name"):
        area = group["area"].iloc[0]
        equipo = group["equipo"].iloc[0]
        registros = group[
            ["Día", "Entrada", "Salida", "Total Diario", "Nota"]
        ].values.tolist()

        dias_con_registro = sum(
            1 for _, entrada, salida, _, nota in registros
            if any([entrada, salida, nota])
        )

        resultado[name.title()] = {
            "area": area,
            "equipo": equipo,
            "registros": registros,
            "dias_con_registro": max(dias_con_registro - 1, 0)
        }

    return resultado


if __name__ == "__main__":
    data = asociar_empleados()

    for nombre, info in data.items():
        print(f"{nombre}: {info['dias_con_registro']} días con registro")


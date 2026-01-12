import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from nombres import extraer_nombres_empleados
from datetime import datetime
import math

# 🔹 Cargar variables de entorno
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")  # Esta ya no será la colección principal, se usarán colecciones por mes

if not all([MONGO_URI, MONGO_DB]):
    raise ValueError("❌ Faltan variables de entorno de MongoDB")

# 🔹 Conexión MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]



def asociar_empleados(file, file_name: str):
    # --- Leer archivo de asistencia ---
    empleado_df, mes_ano = extraer_nombres_empleados(file, file_name)
    empleado_df["name"] = empleado_df["name"].str.strip().str.lower()

    # --- Leer MongoDB ---
    documentos = list(
        db[MONGO_COLLECTION].find(
            {},
            {"_id": 0, "Nombre completo": 1, "Área": 1, "Equipo": 1, "Grupo": 1, "Promedio": 1}
        )
    )

    base_de_datos = pd.DataFrame(documentos)

    if not base_de_datos.empty:
        base_de_datos["name"] = base_de_datos["Nombre completo"].str.strip().str.lower()
    else:
        base_de_datos = pd.DataFrame(columns=["Nombre completo", "Área", "Equipo", "Grupo", "Promedio"])

    # --- Combinar ---
    combinado = pd.merge(empleado_df, base_de_datos, on="name", how="left")
    combinado["Área"] = combinado["Área"].fillna("Sin área")
    combinado["Equipo"] = combinado["Equipo"].fillna("Sin equipo")

    resultado = []

    for name, group in combinado.groupby("name"):
        total_minutos = 0
        hours_per_day = {}

        # --- Generar días secuenciales desde 01-mes_ano ---
        for i, tiempo in enumerate(group["Total Diario"], start=1):
            dia_val = f"{i:02d}-{mes_ano}"  # "01-MM/YYYY", "02-MM/YYYY", etc.

            if isinstance(tiempo, str) and ":" in tiempo:
                h, m = map(int, tiempo.split(":"))
                minutos_dia = h * 60 + m
            else:
                minutos_dia = 0

            total_minutos += minutos_dia
            hours_per_day[dia_val] = f"{minutos_dia // 60:02d}:{minutos_dia % 60:02d}"

        horas_total = f"{total_minutos // 60:02d}:{total_minutos % 60:02d}"

        # --- Días con registro ---
        registros = group[["Entrada", "Salida", "Nota"]].values
        dias_con_registro = sum(
            1 for entrada, salida, nota in registros
            if any([entrada, salida, nota]) and nota != "Falta SALIDA"
        )

        porcentaje = round(
            (dias_con_registro / len(group) * 100), 2
        ) if len(group) else 0

        emp_dict = {
            "name": name.title(),
            "hourstotal": horas_total,
            "hoursperday": hours_per_day,
            "attendance": porcentaje,
            "mes_ano": mes_ano,
        }

        resultado.append(emp_dict)

    return resultado, mes_ano




def subir_a_mongo(data, mes_ano):
    # Crear o usar colección con el nombre del mes
    coleccion_mes = db[mes_ano]

    if data:
        # Insertar registros
        coleccion_mes.insert_many(data)
        return (f"✅ Se subieron {len(data)} registros a la colección '{mes_ano}'")
    else:
        return ("⚠️ No hay datos para subir")


if __name__ == "__main__":
    data, mes_ano = asociar_empleados()
    subir_a_mongo(data, mes_ano)

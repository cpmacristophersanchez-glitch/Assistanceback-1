import os
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import pprint

def leer_alumnos(mongo_uri=None, mongo_db=None):
    """
    Lee la colección 'Employee' y todas las colecciones mensuales de la base de datos,
    y retorna un diccionario con los datos personales y los datos de asistencia por mes.

    Estructura devuelta:
    {
        'Nombre Completo': {
            'datos personales': {...},
            'october_2025': {'attendance': ..., 'hourstotal': ..., 'hoursperday': {...}},
            'september_2025': {...},
            ...
        }
    }
    """

    # 🔹 Cargar variables de entorno si no se pasan como argumento
    load_dotenv()
    MONGO_URI = mongo_uri or os.getenv("MONGO_URI")
    MONGO_DB = mongo_db or os.getenv("MONGO_DB")

    if not all([MONGO_URI, MONGO_DB]):
        raise ValueError("❌ Falta la URI o el nombre de la base de datos de MongoDB")

    # 🔹 Conexión a MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # 🔹 Leer empleados base
    empleados_docs = list(db["Employee"].find({}, {"_id": 0}))
    empleados_df = pd.DataFrame(empleados_docs)
    empleados_df["name"] = empleados_df["Nombre completo"].str.strip().str.lower()

    # 🔹 Obtener todas las colecciones que no sean 'Employee' ni 'AdminUsers'
    colecciones = [c for c in db.list_collection_names() if c not in ["Employee", "AdminUsers"]]

    # 🔹 Diccionario final con datos personales y datos por mes
    datos_por_empleado = {}

    for coleccion_nombre in colecciones:
        coleccion = db[coleccion_nombre]
        docs_mes = list(coleccion.find({}, {"_id": 0}))
        df_mes = pd.DataFrame(docs_mes)
        if df_mes.empty:
            continue

        df_mes["name"] = df_mes["name"].str.strip().str.lower()

        # Unir info mensual con info base del empleado
        df_combinado = pd.merge(df_mes, empleados_df, on="name", how="left")

        for _, fila in df_combinado.iterrows():
            nombre = fila["name"].title()  # Normalizar mayúsculas
            mes = fila["mes_ano"]

            # Datos personales sacados de Employee
            datos_personales = {
                "Nivel": fila.get("Nivel", "N/A"),
                "Turno": fila.get("Turno", "N/A"),
                "Grupo": fila.get("Grupo", "N/A"),
                "Equipo": fila.get("Equipo", "N/A"),
                "Promedio": fila.get("Promedio", "N/A"),
                "Área": fila.get("Área", "N/A")
            }

            # Datos mensuales: ahora con hourstotal y hoursperday
            info_mes = {
                "attendance": fila.get("attendance", 0),
                "hourstotal": fila.get("hourstotal", "00:00"),
                "hoursperday": fila.get("hoursperday", {})  # dict con cada día
            }

            if nombre not in datos_por_empleado:
                datos_por_empleado[nombre] = {"datos personales": datos_personales}

            datos_por_empleado[nombre][mes] = info_mes

    return datos_por_empleado


# 🔹 Prueba rápida
if __name__ == "__main__":
    alumnos = leer_alumnos()
    pprint.pprint(alumnos)

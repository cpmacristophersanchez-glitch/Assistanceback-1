import pandas as pd
import re
from datetime import datetime

# Opciones de pandas para debug
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def obtener_mes_ano_desde_nombre(file_name: str) -> str:
    """
    Extrae mes y año desde un nombre tipo:
    NGTimereport-20250901-20250930-1335.xlsx
    """
    match = re.search(r'(\d{8})', file_name)
    if not match:
        return "desconocido"

    try:
        fecha = datetime.strptime(match.group(1), "%Y%m%d")
        return fecha.strftime("%B_%Y").lower()
    except ValueError:
        return "desconocido"


def extraer_nombres_empleados(file, file_name: str):
    """
    Extrae datos de empleados de un archivo CSV o Excel recibido como objeto.

    Parámetros:
    - file: archivo tipo BytesIO o similar
    - file_name: nombre del archivo (para extraer mes/año)

    Retorna:
    - DataFrame con columnas:
      ['name', 'Día', 'Entrada', 'Salida', 'Total Diario', 'Nota']
    - mes_ano (ej. 'september_2025')
    """

    mes_ano = obtener_mes_ano_desde_nombre(file_name)

    try:
        # Leer según extensión
        if file_name.endswith(".csv"):
            df = pd.read_csv(file, header=None)
        else:
            df = pd.read_excel(file, header=None)

        registros = []

        # Filas que contienen "Empleado"
        filas_empleado = df[df.apply(
            lambda row: row.astype(str).str.contains("Empleado", case=False, na=False).any(),
            axis=1
        )].index

        # Filas que contienen días de la semana
        dias_semana = ["LU", "M", "X", "J", "V", "S", "D"]
        filas_dias = df[df.apply(
            lambda row: any(row.astype(str).str.contains(dia, case=False, na=False).any()
                            for dia in dias_semana),
            axis=1
        )].index

        for i, idx_empleado in enumerate(filas_empleado):
            fila_empleado = df.loc[idx_empleado]
            col_empleado = None

            for j, valor in enumerate(fila_empleado):
                if pd.notna(valor) and "empleado" in str(valor).lower():
                    col_empleado = j
                    break

            if col_empleado is None or col_empleado + 3 >= len(fila_empleado):
                continue

            nombre_raw = fila_empleado[col_empleado + 3]
            nombre_limpio = re.sub(r'\([^)]*\)', '', str(nombre_raw)).strip()

            if not nombre_limpio or nombre_limpio.lower() in ["nan", "none", ""]:
                continue

            inicio_dias = idx_empleado + 1
            fin_dias = filas_empleado[i + 1] if i + 1 < len(filas_empleado) else len(df)

            for idx_dia in range(inicio_dias, fin_dias):
                if idx_dia not in filas_dias:
                    continue

                fila_dia = df.loc[idx_dia]

                dia = entrada = salida = total = nota = None

                # Día (YYYY-MM-DD)
                for valor in fila_dia:
                    if pd.notna(valor) and re.match(r'\d{4}-\d{2}-\d{2}', str(valor)):
                        dia = str(valor)
                        break

                # Entrada
                for valor in fila_dia:
                    if pd.notna(valor) and re.match(r'\d{1,2}:\d{2}', str(valor)):
                        entrada = str(valor)
                        break

                # Salida
                for valor in fila_dia:
                    if pd.notna(valor):
                        val_str = str(valor)
                        if re.match(r'\d{1,2}:\d{2}', val_str) or "falta" in val_str.lower():
                            if val_str != entrada:
                                salida = val_str
                                break

                # Total diario
                for valor in fila_dia:
                    if pd.notna(valor) and re.match(r'\d+:\d{2}', str(valor)):
                        if valor not in [entrada, salida]:
                            total = str(valor)
                            break

                # Nota
                for valor in fila_dia:
                    if pd.notna(valor) and isinstance(valor, str):
                        if any(k in valor.lower() for k in ["nota", "falta", "observacion"]):
                            nota = valor
                            break

                if dia or entrada or salida:
                    registros.append({
                        "name": nombre_limpio,
                        "Día": dia,
                        "Entrada": entrada,
                        "Salida": salida,
                        "Total Diario": total,
                        "Nota": nota
                    })

        df_resultado = pd.DataFrame(registros)
        df_resultado = df_resultado.dropna(how="all")
        df_resultado = df_resultado[df_resultado["name"].notna() & (df_resultado["name"] != "")]

        return df_resultado.reset_index(drop=True), mes_ano

    except Exception as e:
        print(f"❌ Error procesando el archivo: {e}")
        return (
            pd.DataFrame(columns=["name", "Día", "Entrada", "Salida", "Total Diario", "Nota"]),
            "desconocido"
        )

# import os
# import sys
# proyecto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(proyecto_dir)
# from config.dbconfig import session
# from models.evento_reuniones import Evento
# from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

# df = pd.read_csv("./csvs/eventos_procesados_20250702_102141.csv")

# def insertar_dataframe_en_mysql(df, session):
#     print("\n--- Intentando insertar datos en la base de datos MySQL ---")
#     if df.empty:
#         print("El DataFrame está vacío. No hay datos para insertar.")
#         return

#     rows_inserted = 0
#     for index, row in df.iterrows():
#         try:
#             evento = Evento(
#                 nombre=row['nombre'],
#                 tipo=row['tipo'],
#                 detalle_tipo_rotacion=row['detalle_tipo_rotacion'],
#                 tema=row['tema'],
#                 fecha_edicion=row['fecha_edicion'],
#                 fecha_inicio=row['fecha_inicio'],
#                 fecha_fin=row['fecha_fin'],
#                 anio=row['anio'],
#                 mes=row['mes'],
#                 dia_inicio=row['dia_inicio'],
#                 dia_fin=row['dia_fin'],
#                 fecha_texto=row['fecha_texto']
#             )

#             session.add(evento)
#             rows_inserted += 1

#             if rows_inserted % 10 == 0:  # Comitear cada 50 registros
#                 session.commit()
#                 print(f"--> {rows_inserted} registros insertados y comiteados.")

#         except SQLAlchemyError as e:
#             session.rollback()  # Si hay un error, revierte la transacción actual
#             print(f"Error al insertar registro {index}: {e}. Revirtiendo...")
#             print(f"Datos del registro problemático: {row.to_dict()}")
#             continue  # Continúa con el siguiente registro
#         except Exception as e:
#             print(f"Error inesperado al insertar registro {index}: {e}")
#             print(f"Datos del registro problemático: {row.to_dict()}")
#             continue

#     try:
#         session.commit()  # Comitear los registros restantes
#         print(
#             f"\n¡Inserción completada! Se insertaron un total de {rows_inserted} registros.")
#     except SQLAlchemyError as e:
#         session.rollback()
#         print(f"Error final al comitear: {e}. Revirtiendo...")

# insertar_dataframe_en_mysql(df=df, session=session)}

df = pd.read_csv("organizadores - organizadores (1).csv", low_memory=False)

df["Entidad organizadores"] = df["Entidad organizadores"].astype(str).str.strip().str.split(" - ").str[0]

df.to_csv("entidades_fix.csv", sep=";", index=False)
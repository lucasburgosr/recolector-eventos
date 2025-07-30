import pandas as pd

df = pd.read_csv("eventos_sin_na.csv", sep=";", low_memory=False)

# Crear columna 'agrupacion' solo para las filas que cumplan la condición
df.loc[df["tipoEvento"] == "Evento Cultural", "agrupacion"] = "FUERA DEL ALCANCE DEL OETR"
df.loc[df["tipoEvento"] == "Evento Deportivo Nacional", "agrupacion"] = "FUERA DEL ALCANCE DEL OETR"

df.to_csv("eventos_casi_final.csv", sep=";", index=False)
import pandas as pd

df = pd.read_csv("./eventos_turismo_reuniones.csv", sep=";", low_memory=False)

df_filtrado = df[df["Verificación"] == "VERDADERO"]

df_filtrado.to_csv("./eventos_verificados_filtrados.csv", sep=";", index=False)
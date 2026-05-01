import pandas as pd


arquivo = "data/complicacao_ajustada/complicacao.xlsx"

df = pd.read_excel(arquivo, sheet_name="BASE")
df["COD USUARIO"] = df["COD USUARIO"].fillna("").astype(str).str.strip()

df = df[df["COD USUARIO"] != ""]

duplicados = df[df.duplicated(subset="COD USUARIO", keep=False)]
qtd_codigos_duplicados = duplicados["COD USUARIO"].nunique()

duplicados.to_excel("cod_usuario_duplicados.xlsx", index=False)

print("Linhas com COD USUARIO duplicado:", len(duplicados))
print("Quantidade de COD USUARIO duplicados:", qtd_codigos_duplicados)

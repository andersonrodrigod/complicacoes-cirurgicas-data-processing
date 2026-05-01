from pathlib import Path

import pandas as pd

from configuracoes import ABAS_AJUSTADAS, COLUNAS_PRINCIPAIS


arquivo = Path("data/complicacao.xlsx")
pasta_saida = Path("data/complicacao_ajustada")
arquivo_saida = pasta_saida / "complicacao.xlsx"


def remover_parto_laqueadura(df):
    # Remove procedimentos de parto e laqueadura da base ajustada e separa as linhas excluidas para conferencia.
    procedimento = df["PROCEDIMENTO"].fillna("").astype(str).str.upper().str.strip()
    mascara_excluir = procedimento.str.contains("PARTO|LAQUEADURA", na=False)

    df_filtrado = df[~mascara_excluir].copy()
    df_excluidos = df[mascara_excluir].copy()

    return df_filtrado, df_excluidos


df = pd.read_excel(arquivo, sheet_name=0)

for coluna in COLUNAS_PRINCIPAIS:
    if coluna not in df.columns:
        df[coluna] = ""

df["IDADE T"] = (
    df["IDADE"].astype("string").str.extract(r"^\s*([^Aa]*)[Aa]", expand=False).str.strip().fillna("")
)

df, df_excluidos_parto_laqueadura = remover_parto_laqueadura(df)

colunas_extras = [coluna for coluna in df.columns if coluna not in COLUNAS_PRINCIPAIS]
df = df[COLUNAS_PRINCIPAIS + colunas_extras]

pasta_saida.mkdir(parents=True, exist_ok=True)

with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="BASE", index=False)

    for nome_aba, colunas in ABAS_AJUSTADAS.items():
        df_aba = pd.DataFrame(columns=colunas)
        df_aba.to_excel(writer, sheet_name=nome_aba, index=False)

arquivo_excluidos = pasta_saida / "linhas_excluidas_parto_laqueadura.xlsx"
df_excluidos_parto_laqueadura.to_excel(arquivo_excluidos, index=False)

print(f"Planilha organizada com sucesso em: {arquivo_saida}")
print(f"Linhas removidas com PARTO ou LAQUEADURA: {len(df_excluidos_parto_laqueadura)}")
print(f"Arquivo com linhas removidas: {arquivo_excluidos}")

import pandas as pd
import re
from decimal import Decimal, InvalidOperation

# =========================================================
# 1. Ler os arquivos
# =========================================================
arquivo_entrada = "REGISTROS_MANTIDOS.xlsx"
arquivo_saida = "COMPLICACAO MARCO.xlsx"
aba_base = "BASE"

abas = pd.read_excel(arquivo_entrada, sheet_name=None)
df_main = abas[aba_base]
df_senhas = pd.read_csv("telefone_marco_internacoes.csv")

# =========================================================
# 2. Limpar e padronizar chave SENHA
# =========================================================
def normalizar_chave(v) -> str:
    if pd.isna(v):
        return ""
    texto = str(v).strip()
    if texto.lower() in {"nan", "none", "<na>"}:
        return ""
    # Remove apenas sufixo decimal sem valor real: 123.0 -> 123
    if re.fullmatch(r"\d+\.0+", texto):
        return texto.split(".")[0]
    return texto

df_main["SENHA"] = df_main["SENHA"].apply(normalizar_chave)
df_senhas["CD_SENHA"] = df_senhas["CD_SENHA"].apply(normalizar_chave)

# =========================================================
# 3. Selecionar apenas colunas necessárias do CSV
# =========================================================
telefones = ["TELEFONE_1", "TELEFONE_2", "TELEFONE_3", "TELEFONE_4", "TELEFONE_5"]

colunas_csv = ["CD_SENHA"] + telefones + ["CD_PESSOA"]
df_senhas = df_senhas[colunas_csv]

# =========================================================
# 4. Limpar telefones (preserva NaN reais)
# =========================================================
def normalizar_telefone(v) -> str:
    if pd.isna(v):
        return ""
    if isinstance(v, bool):
        return ""
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if pd.isna(v):
            return ""
        # Evita bug de adicionar zero no final quando vem como 1234567890.0
        if float(v).is_integer():
            return str(int(v))
        texto_float = format(v, "f").rstrip("0").rstrip(".")
        return re.sub(r"\D", "", texto_float)

    texto = str(v).strip()
    if texto.lower() in {"nan", "none", "<na>"}:
        return ""

    # Trata strings numericas (inclusive notacao cientifica) sem criar zero extra.
    texto_num = texto.replace(",", ".")
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", texto_num):
        try:
            valor = Decimal(texto_num)
            if valor == valor.to_integral_value():
                return str(int(valor))
        except (InvalidOperation, ValueError):
            pass

    # Fallback para formatos com mascara/sinais.
    return re.sub(r"\D", "", texto)

for col in telefones:
    df_senhas[col] = df_senhas[col].apply(normalizar_telefone)

# =========================================================
# 5. Adicionar prefixo 55 (somente quando existir)
# =========================================================
for col in telefones:
    df_senhas[col] = df_senhas[col].apply(
        lambda x: f"55{x}" if x != "" and not x.startswith("55") else x
    )

# =========================================================
# 5.1 Regra adicional para telefones com nono dígito
# =========================================================
def ajustar_nono_digito(telefone: str) -> str:
    if telefone == "":
        return telefone

    # Numero fixo: contando da direita para a esquerda, o 8o digito
    # em 2, 3, 4 ou 5 indica que nao deve inserir 9.
    if len(telefone) >= 8:
        oitavo_da_direita = telefone[-8]
        if oitavo_da_direita in {"2", "3", "4", "5"}:
            return telefone

    # Regra atual: apenas para telefones com 12 caracteres,
    # inserir 9 apos o 4o caractere.
    if len(telefone) == 12:
        return telefone[:4] + "9" + telefone[4:]

    return telefone

for col in telefones:
    df_senhas[col] = df_senhas[col].apply(ajustar_nono_digito)

# =========================================================
# 5.2 Deduplicar CD_SENHA antes do merge
# =========================================================
def primeiro_nao_vazio(serie: pd.Series) -> str:
    for v in serie:
        if isinstance(v, str) and v != "":
            return v
    return ""

aggs = {col: primeiro_nao_vazio for col in telefones}
aggs["CD_PESSOA"] = "first" #type: ignore
df_senhas = (
    df_senhas
    .groupby("CD_SENHA", as_index=False, dropna=False)
    .agg(aggs)
)

# =========================================================
# 6. Merge mantendo todas as linhas da base principal
# =========================================================
df_final = df_main.merge(
    df_senhas,
    how="left",
    left_on="SENHA",
    right_on="CD_SENHA"
)

# =========================================================
# 7. Remover coluna duplicada de chave
# =========================================================
df_final = df_final.drop(columns=["CD_SENHA"])

# =========================================================
# 8. Garantir telefones vazios no resultado final
# =========================================================
df_final[telefones] = (
    df_final[telefones]
    .replace(["nan", "None", "<NA>"], "")
    .fillna("")
)

# Limpeza final defensiva: garante que nao saia valor com ".0".
for col in telefones:
    df_final[col] = df_final[col].apply(normalizar_telefone)

# =========================================================
# 9. Flags e contagens
# =========================================================
df_final["ENCONTROU"] = df_final[telefones].ne("").any(axis=1) # type: ignore
print("Total de SENHAS encontradas (com telefone):", df_final["ENCONTROU"].sum())

df_final["MATCH_SENHA"] = df_final["SENHA"].isin(df_senhas["CD_SENHA"])
print("Match por SENHA:", df_final["MATCH_SENHA"].sum())

sem_telefone = df_final[
    (df_final["MATCH_SENHA"] == True) &
    (df_final[telefones].eq("").all(axis=1)) # type: ignore
]
print("Senha encontrada, mas sem telefone:", len(sem_telefone))

# =========================================================
# 10. Salvar arquivo final
# =========================================================
abas[aba_base] = df_final

with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
    for nome_aba, df_aba in abas.items():
        df_aba.to_excel(writer, sheet_name=nome_aba, index=False)

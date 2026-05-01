import pandas as pd
import re


# =====================================================
# Funcoes auxiliares
# =====================================================
def primeira_palavra(valor):
    texto = str(valor).strip().upper()
    if not texto:
        return ""
    return texto.split()[0]


PALAVRAS_PRIORITARIAS = [
    # Prioridades originais
    "CESARIANA",
    "PARTO",
    # Primeiras palavras da coluna PROC_1 (fixas no codigo)
    "ABLACAO",
    "AMIGDALECTOMIA",
    "AMPUTACAO",
    "ANEURISMA",
    "ANGIOPLASTIA",
    "ANOMALIA",
    "APENDICECTOMIA",
    "ARTRITE",
    "ARTRODESE",
    "ARTROPLASTIA",
    "ARTROTOMIA",
    "BRONCOSCOPIA",
    "COLECISTECTOMIA",
    "COLECTOMIA",
    "COLPOPLASTIA",
    "DILATACAO",
    "ENXERTO",
    "EPISTAXE",
    "FACECTOMIA",
    "FISTULECTOMIA",
    "GASTROPLASTIA",
    "HERNIA",
    "HERNIORRAFIA",
    "HISTERECTOMIA",
    "HISTEROSCOPIA",
    "LAPAROTOMIA",
    "LIPOASPIRACAO",
    "MAMOPLASTIA",
    "ORQUIDOPEXIA",
    "OSTEOPLASTIAS",
    "PTERIGIO",
    "QUADRANTECTOMIA",
    "QUIMIOEMBOLIZACAO",
    "RECONSTRUCAO",
    "RUPTURA",
    "SEPTOPLASTIA",
    "TROCA",
    "URETERORRENOLITOTRIPSIA",
]


# =====================================================
# 1. Ler arquivo
# =====================================================
arquivo_entrada = "BASE_SEM_PARTO_LAQUEADURA.xlsx"
arquivo_saida = "REGISTROS_MANTIDOS.xlsx"
arquivo_excluidos = "REGISTROS_EXCLUIDOS.xlsx"
aba_base = "BASE"

abas = pd.read_excel(arquivo_entrada, sheet_name=None)
df = abas[aba_base]

# Padronizar colunas
df["COD USUARIO"] = df["COD USUARIO"].astype(str).str.strip()
df["PROCEDIMENTO"] = df["PROCEDIMENTO"].astype(str).str.upper().str.strip()
padrao_prioridades = r"\b(?:" + "|".join(re.escape(p) for p in PALAVRAS_PRIORITARIAS) + r")\b"

# =====================================================
# 2. Separar duplicados
# =====================================================
duplicados = df[df["COD USUARIO"].duplicated(keep=False)]
nao_duplicados = df[~df["COD USUARIO"].duplicated(keep=False)]

mantidos = []
excluidos = []

# =====================================================
# 3. Processar cada codigo duplicado (REGRAS)
# =====================================================
for codigo, grupo in duplicados.groupby("COD USUARIO"):

    # --- REGRA PRIORITARIA POR PRIMEIRA PALAVRA ---
    mascara_prioridade = grupo["PROCEDIMENTO"].str.contains(padrao_prioridades, na=False, regex=True)

    if mascara_prioridade.any():
        manter = grupo[mascara_prioridade]
        excluir = grupo.drop(manter.index)

    # --- REGRA: INTERNACAO (misto) ---
    elif grupo["PROCEDIMENTO"].str.contains("INTERNACAO", na=False).any():
        manter = grupo[~grupo["PROCEDIMENTO"].str.contains("INTERNACAO", na=False)]
        excluir = grupo.drop(manter.index)

        # Se todos forem internacao, mantem todos (por enquanto)
        if manter.empty:
            manter = grupo
            excluir = grupo.iloc[0:0]

    else:
        manter = grupo
        excluir = grupo.iloc[0:0]

    mantidos.append(manter)
    excluidos.append(excluir)

# =====================================================
# 4. Consolidar apos regras principais
# =====================================================
df_mantidos = pd.concat(mantidos + [nao_duplicados], ignore_index=True)
df_excluidos = pd.concat(excluidos, ignore_index=True)

# =====================================================
# 5. REGRA FINAL: duplicados restantes so com INTERNACAO
# =====================================================
duplicados_finais = df_mantidos[
    df_mantidos["COD USUARIO"].duplicated(keep=False)
]

for codigo, grupo in duplicados_finais.groupby("COD USUARIO"):

    # Se TODOS os procedimentos forem INTERNACAO
    if grupo["PROCEDIMENTO"].str.contains("INTERNACAO", na=False).all():

        # Manter apenas 1 (o primeiro)
        excluir_restante = grupo.iloc[1:]

        # Atualizar mantidos e excluidos
        df_mantidos = df_mantidos.drop(excluir_restante.index)
        df_excluidos = pd.concat(
            [df_excluidos, excluir_restante],
            ignore_index=True
        )

# =====================================================
# 6. REGRAS EXTRAS (apos regra por palavra)
# =====================================================
# 6.1 Em CD_USUARIO duplicado, se PROCEDIMENTO repetir exatamente,
#     mantem o primeiro e descarta os demais.
mascara_cd_dup = df_mantidos["COD USUARIO"].duplicated(keep=False)
mascara_proc_exato = df_mantidos.duplicated(
    subset=["COD USUARIO", "PROCEDIMENTO"], keep="first"
)
descartar_proc_exato = mascara_cd_dup & mascara_proc_exato

linhas_descartadas_exatas = df_mantidos[descartar_proc_exato]
df_mantidos = df_mantidos[~descartar_proc_exato]
df_excluidos = pd.concat([df_excluidos, linhas_descartadas_exatas], ignore_index=True)

# 6.2 Em CD_USUARIO duplicado, se a primeira palavra do PROCEDIMENTO repetir,
#     mantem o primeiro e descarta os demais.
df_mantidos["_P1_PROC"] = df_mantidos["PROCEDIMENTO"].apply(primeira_palavra)
mascara_cd_dup_2 = df_mantidos["COD USUARIO"].duplicated(keep=False)
mascara_p1_repete = df_mantidos.duplicated(
    subset=["COD USUARIO", "_P1_PROC"], keep="first"
)
descartar_primeira_palavra = mascara_cd_dup_2 & mascara_p1_repete

linhas_descartadas_p1 = df_mantidos[descartar_primeira_palavra].drop(columns=["_P1_PROC"])
df_mantidos = df_mantidos[~descartar_primeira_palavra].drop(columns=["_P1_PROC"])
df_excluidos = pd.concat([df_excluidos, linhas_descartadas_p1], ignore_index=True)

# =====================================================
# 7. Salvar arquivos finais
# =====================================================
abas[aba_base] = df_mantidos

with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
    for nome_aba, df_aba in abas.items():
        df_aba.to_excel(writer, sheet_name=nome_aba, index=False)

df_excluidos.to_excel(arquivo_excluidos, index=False)

print("Processo finalizado.")
print("Mantidos:", df_mantidos.shape[0])
print("Excluidos:", df_excluidos.shape[0])
print("Palavras prioritarias configuradas:", len(PALAVRAS_PRIORITARIAS))

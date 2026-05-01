from pathlib import Path
import re
from decimal import Decimal, InvalidOperation

import pandas as pd

from configuracoes import ABAS_AJUSTADAS, COLUNAS_PRINCIPAIS, PALAVRAS_PRIORITARIAS
from formulas_excel import aplicar_formulas


arquivo = Path("data/complicacao.xlsx")
arquivo_telefones = Path("data/telefone_abril_internacoes.csv")
pasta_saida = Path("data/complicacao_ajustada")
pasta_relatorios = Path("data/relatorios_processamento")
arquivo_saida = pasta_saida / "complicacao.xlsx"
colunas_telefone = {
    "TELEFONE_1": "TELEFONE 1",
    "TELEFONE_2": "TELEFONE 2",
    "TELEFONE_3": "TELEFONE 3",
    "TELEFONE_4": "TELEFONE 4",
    "TELEFONE_5": "TELEFONE 5",
}


def primeira_palavra(valor):
    texto = str(valor).strip().upper()
    if not texto:
        return ""
    return texto.split()[0]


def normalizar_chave(valor):
    if pd.isna(valor):
        return ""

    texto = str(valor).strip()
    if texto.lower() in {"nan", "none", "<na>"}:
        return ""

    if re.fullmatch(r"\d+\.0+", texto):
        return texto.split(".")[0]

    return texto


def normalizar_telefone(valor):
    if pd.isna(valor):
        return ""
    if isinstance(valor, bool):
        return ""
    if isinstance(valor, int):
        return str(valor)
    if isinstance(valor, float):
        if pd.isna(valor):
            return ""
        if float(valor).is_integer():
            return str(int(valor))
        texto_float = format(valor, "f").rstrip("0").rstrip(".")
        return re.sub(r"\D", "", texto_float)

    texto = str(valor).strip()
    if texto.lower() in {"nan", "none", "<na>"}:
        return ""

    texto_num = texto.replace(",", ".")
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", texto_num):
        try:
            valor_decimal = Decimal(texto_num)
            if valor_decimal == valor_decimal.to_integral_value():
                return str(int(valor_decimal))
        except (InvalidOperation, ValueError):
            pass

    return re.sub(r"\D", "", texto)


def ajustar_nono_digito(telefone):
    if telefone == "":
        return telefone

    if len(telefone) >= 8:
        oitavo_da_direita = telefone[-8]
        if oitavo_da_direita in {"2", "3", "4", "5"}:
            return telefone

    if len(telefone) == 12:
        return telefone[:4] + "9" + telefone[4:]

    return telefone


def primeiro_nao_vazio(serie):
    for valor in serie:
        if isinstance(valor, str) and valor != "":
            return valor
    return ""


def validar_merge_sem_duplicar_linhas(qtd_antes, df_depois, nome_merge):
    qtd_depois = len(df_depois)
    if qtd_depois != qtd_antes:
        raise ValueError(
            f"O merge por {nome_merge} alterou a quantidade de linhas da base "
            f"({qtd_antes} -> {qtd_depois}). Verifique duplicidades na chave antes de continuar."
        )


def ordenar_por_data_internacao(df):
    df = df.copy()
    data_ordenacao = pd.to_datetime(df["DT INTERNACAO"], errors="coerce", dayfirst=True)
    df["_DT_INTERNACAO_ORDENACAO"] = data_ordenacao
    df = df.sort_values(
        by=["_DT_INTERNACAO_ORDENACAO", "COD USUARIO"],
        ascending=[True, True],
        na_position="last",
    )
    return df.drop(columns=["_DT_INTERNACAO_ORDENACAO"]).reset_index(drop=True)


def sinalizar_duplicidade(df):
    df = df.copy()
    codigo_usuario = df["COD USUARIO"].fillna("").astype(str).str.strip()
    df["DUPLICIDADE"] = codigo_usuario.ne("") & codigo_usuario.duplicated(keep=False)
    return df


def adicionar_motivo_exclusao(df, motivo):
    df = df.copy()
    df["MOTIVO_EXCLUSAO"] = motivo
    return df


def juntar_excluidos(lista_excluidos, colunas_base):
    lista_validada = [df for df in lista_excluidos if not df.empty]
    colunas_excluidos = list(colunas_base) + ["MOTIVO_EXCLUSAO"]

    if not lista_validada:
        return pd.DataFrame(columns=colunas_excluidos)

    return pd.concat(lista_validada, ignore_index=True).reindex(columns=colunas_excluidos)


def remover_parto_laqueadura(df):
    # Remove procedimentos de parto e laqueadura da base ajustada e separa as linhas excluidas para conferencia.
    procedimento = df["PROCEDIMENTO"].fillna("").astype(str).str.upper().str.strip()
    mascara_excluir = procedimento.str.contains("PARTO|LAQUEADURA", na=False)

    df_filtrado = df[~mascara_excluir].copy()
    df_excluidos = adicionar_motivo_exclusao(df[mascara_excluir], "PARTO OU LAQUEADURA")

    return df_filtrado, df_excluidos


def adicionar_telefones_por_senha(df, caminho_csv):
    # Enriquece a base com telefones por SENHA quando o CSV estiver disponivel em data/.
    if not caminho_csv.exists():
        resumo = {
            "executado": False,
            "motivo": f"Arquivo de telefones nao encontrado: {caminho_csv}",
        }
        return df, resumo

    colunas_telefone_csv = list(colunas_telefone)
    colunas_telefone_final = list(colunas_telefone.values())
    cabecalho = pd.read_csv(caminho_csv, nrows=0)
    coluna_senha_csv = "CD_SENHA" if "CD_SENHA" in cabecalho.columns else "CD_SENHA_AUTORIZA"
    coluna_usuario_csv = "CD_USUARIO" if "CD_USUARIO" in cabecalho.columns else None
    colunas_obrigatorias = [coluna_senha_csv] + colunas_telefone_csv + ["CD_PESSOA"]
    colunas_csv = colunas_obrigatorias + ([coluna_usuario_csv] if coluna_usuario_csv else [])
    colunas_faltantes = [coluna for coluna in colunas_obrigatorias if coluna not in cabecalho.columns]

    df_final = df.copy()
    if colunas_faltantes:
        resumo = {
            "executado": False,
            "motivo": f"CSV de telefones sem colunas obrigatorias: {', '.join(colunas_faltantes)}",
        }
        return df_final, resumo

    df_final["SENHA"] = df_final["SENHA"].apply(normalizar_chave)
    df_final["COD USUARIO"] = df_final["COD USUARIO"].apply(normalizar_chave)
    senhas_base = set(df_final["SENHA"])
    codigos_usuario_base = set(df_final["COD USUARIO"])
    df_senhas = pd.read_csv(caminho_csv, usecols=colunas_csv, dtype=str, low_memory=False)
    df_senhas[coluna_senha_csv] = df_senhas[coluna_senha_csv].apply(normalizar_chave)
    mascara_csv = df_senhas[coluna_senha_csv].isin(senhas_base)

    if coluna_usuario_csv:
        df_senhas[coluna_usuario_csv] = df_senhas[coluna_usuario_csv].apply(normalizar_chave)
        mascara_csv = mascara_csv | df_senhas[coluna_usuario_csv].isin(codigos_usuario_base)

    df_senhas = df_senhas[mascara_csv]
    df_senhas = df_senhas.rename(columns={coluna_senha_csv: "CD_SENHA"})
    if coluna_usuario_csv:
        df_senhas = df_senhas.rename(columns={coluna_usuario_csv: "CD_USUARIO"})

    for coluna in colunas_telefone_csv:
        df_senhas[coluna] = df_senhas[coluna].apply(normalizar_telefone)
        df_senhas[coluna] = df_senhas[coluna].apply(
            lambda telefone: f"55{telefone}" if telefone != "" and not telefone.startswith("55") else telefone
        )
        df_senhas[coluna] = df_senhas[coluna].apply(ajustar_nono_digito)

    agregacoes = {coluna: primeiro_nao_vazio for coluna in colunas_telefone_csv}
    agregacoes["CD_PESSOA"] = primeiro_nao_vazio
    df_senhas_por_senha = df_senhas.groupby("CD_SENHA", as_index=False, dropna=False).agg(agregacoes)
    df_senhas_por_senha["_MATCH_SENHA"] = True
    df_senhas_por_senha = df_senhas_por_senha.rename(columns={"CD_PESSOA": "_CD_PESSOA_SENHA"})

    qtd_linhas_antes_merge = len(df_final)
    df_final = df_final.merge(
        df_senhas_por_senha,
        how="left",
        left_on="SENHA",
        right_on="CD_SENHA",
    )
    validar_merge_sem_duplicar_linhas(qtd_linhas_antes_merge, df_final, "SENHA")
    df_final = df_final.drop(columns=["CD_SENHA"])
    df_final["MATCH_SENHA"] = df_final["_MATCH_SENHA"].eq(True)
    df_final = df_final.drop(columns=["_MATCH_SENHA"])

    cd_pessoa_atual = df_final["CD_PESSOA"].fillna("").astype(str).str.strip()
    cd_pessoa_csv = df_final["_CD_PESSOA_SENHA"].fillna("").astype(str).str.strip()
    df_final["CD_PESSOA"] = cd_pessoa_atual.where(cd_pessoa_atual != "", cd_pessoa_csv)
    df_final = df_final.drop(columns=["_CD_PESSOA_SENHA"])

    for coluna_csv, coluna_final in colunas_telefone.items():
        telefone_atual = df_final[coluna_final].fillna("").astype(str).str.strip()
        telefone_csv = df_final[coluna_csv].fillna("").astype(str).str.strip()
        df_final[coluna_final] = telefone_atual.where(telefone_atual != "", telefone_csv)

    df_final = df_final.drop(columns=colunas_telefone_csv)

    df_final["MATCH_COD_USUARIO"] = False
    if coluna_usuario_csv:
        # Match temporario: usado para recuperar telefones quando a query do CSV nao traz a CD_SENHA correta.
        # Se a query SQL passar a retornar a verdadeira CD_SENHA, este fallback por COD USUARIO pode ser removido.
        df_senhas_por_usuario = (
            df_senhas[df_senhas["CD_USUARIO"] != ""]
            .groupby("CD_USUARIO", as_index=False, dropna=False)
            .agg(agregacoes)
        )
        df_senhas_por_usuario["_MATCH_COD_USUARIO"] = True
        df_senhas_por_usuario = df_senhas_por_usuario.rename(
            columns={
                "CD_PESSOA": "_CD_PESSOA_USUARIO",
                **{coluna: f"_USUARIO_{coluna}" for coluna in colunas_telefone_csv},
            }
        )

        qtd_linhas_antes_merge = len(df_final)
        df_final = df_final.merge(
            df_senhas_por_usuario,
            how="left",
            left_on="COD USUARIO",
            right_on="CD_USUARIO",
        )
        validar_merge_sem_duplicar_linhas(qtd_linhas_antes_merge, df_final, "COD USUARIO")
        df_final = df_final.drop(columns=["CD_USUARIO"])
        df_final["MATCH_COD_USUARIO"] = df_final["_MATCH_COD_USUARIO"].eq(True)
        df_final = df_final.drop(columns=["_MATCH_COD_USUARIO"])

        cd_pessoa_atual = df_final["CD_PESSOA"].fillna("").astype(str).str.strip()
        cd_pessoa_usuario = df_final["_CD_PESSOA_USUARIO"].fillna("").astype(str).str.strip()
        df_final["CD_PESSOA"] = cd_pessoa_atual.where(cd_pessoa_atual != "", cd_pessoa_usuario)
        df_final = df_final.drop(columns=["_CD_PESSOA_USUARIO"])

        for coluna_csv, coluna_final in colunas_telefone.items():
            coluna_usuario = f"_USUARIO_{coluna_csv}"
            telefone_atual = df_final[coluna_final].fillna("").astype(str).str.strip()
            telefone_usuario = df_final[coluna_usuario].fillna("").astype(str).str.strip()
            df_final[coluna_final] = telefone_atual.where(telefone_atual != "", telefone_usuario)

        df_final = df_final.drop(columns=[f"_USUARIO_{coluna}" for coluna in colunas_telefone_csv])

    df_final[colunas_telefone_final] = (
        df_final[colunas_telefone_final]
        .replace(["nan", "None", "<NA>"], "")
        .fillna("")
    )

    for coluna in colunas_telefone_final:
        df_final[coluna] = df_final[coluna].apply(normalizar_telefone)

    df_final["ENCONTROU"] = df_final[colunas_telefone_final].ne("").any(axis=1)
    sem_telefone = df_final[
        ((df_final["MATCH_SENHA"] == True) | (df_final["MATCH_COD_USUARIO"] == True)) &
        (df_final[colunas_telefone_final].eq("").all(axis=1))
    ]

    resumo = {
        "executado": True,
        "senhas_com_telefone": int(df_final["ENCONTROU"].sum()),
        "match_senha": int(df_final["MATCH_SENHA"].sum()),
        "match_cod_usuario": int(df_final["MATCH_COD_USUARIO"].sum()),
        "match_sem_telefone": len(sem_telefone),
    }

    return df_final, resumo


def remover_duplicados_por_regras(df):
    # Aplica as regras de duplicidade por COD USUARIO e separa as linhas excluidas para auditoria.
    df = df.copy()
    df["COD USUARIO"] = df["COD USUARIO"].astype(str).str.strip()
    df["PROCEDIMENTO"] = df["PROCEDIMENTO"].astype(str).str.upper().str.strip()

    padrao_prioridades = r"\b(?:" + "|".join(re.escape(p) for p in PALAVRAS_PRIORITARIAS) + r")\b"

    duplicados = df[df["COD USUARIO"].duplicated(keep=False)]
    nao_duplicados = df[~df["COD USUARIO"].duplicated(keep=False)]

    mantidos = []
    excluidos = []

    for codigo, grupo in duplicados.groupby("COD USUARIO"):
        mascara_prioridade = grupo["PROCEDIMENTO"].str.contains(padrao_prioridades, na=False, regex=True)

        if mascara_prioridade.any():
            manter = grupo[mascara_prioridade]
            excluir = grupo.drop(manter.index)
            motivo = "DUPLICADO - MANTIDO PROCEDIMENTO PRIORITARIO"

        elif grupo["PROCEDIMENTO"].str.contains("INTERNACAO", na=False).any():
            manter = grupo[~grupo["PROCEDIMENTO"].str.contains("INTERNACAO", na=False)]
            excluir = grupo.drop(manter.index)
            motivo = "DUPLICADO - INTERNACAO"

            if manter.empty:
                manter = grupo
                excluir = grupo.iloc[0:0]

        else:
            manter = grupo
            excluir = grupo.iloc[0:0]
            motivo = ""

        mantidos.append(manter)

        if not excluir.empty:
            excluidos.append(adicionar_motivo_exclusao(excluir, motivo))

    if mantidos:
        df_mantidos = pd.concat(mantidos + [nao_duplicados], ignore_index=True)
    else:
        df_mantidos = nao_duplicados.copy()

    df_excluidos = juntar_excluidos(excluidos, df.columns)

    duplicados_finais = df_mantidos[df_mantidos["COD USUARIO"].duplicated(keep=False)]

    for codigo, grupo in duplicados_finais.groupby("COD USUARIO"):
        if grupo["PROCEDIMENTO"].str.contains("INTERNACAO", na=False).all():
            excluir_restante = grupo.iloc[1:]

            df_mantidos = df_mantidos.drop(excluir_restante.index)
            df_excluidos = pd.concat(
                [
                    df_excluidos,
                    adicionar_motivo_exclusao(excluir_restante, "DUPLICADO - INTERNACAO REPETIDA"),
                ],
                ignore_index=True,
            )

    mascara_cd_dup = df_mantidos["COD USUARIO"].duplicated(keep=False)
    mascara_proc_exato = df_mantidos.duplicated(
        subset=["COD USUARIO", "PROCEDIMENTO"], keep="first"
    )
    descartar_proc_exato = mascara_cd_dup & mascara_proc_exato

    linhas_descartadas_exatas = df_mantidos[descartar_proc_exato]
    df_mantidos = df_mantidos[~descartar_proc_exato]
    df_excluidos = pd.concat(
        [
            df_excluidos,
            adicionar_motivo_exclusao(linhas_descartadas_exatas, "DUPLICADO - PROCEDIMENTO REPETIDO"),
        ],
        ignore_index=True,
    )

    df_mantidos["_P1_PROC"] = df_mantidos["PROCEDIMENTO"].apply(primeira_palavra)
    mascara_cd_dup_2 = df_mantidos["COD USUARIO"].duplicated(keep=False)
    mascara_p1_repete = df_mantidos.duplicated(
        subset=["COD USUARIO", "_P1_PROC"], keep="first"
    )
    descartar_primeira_palavra = mascara_cd_dup_2 & mascara_p1_repete

    linhas_descartadas_p1 = df_mantidos[descartar_primeira_palavra].drop(columns=["_P1_PROC"])
    df_mantidos = df_mantidos[~descartar_primeira_palavra].drop(columns=["_P1_PROC"])
    df_excluidos = pd.concat(
        [
            df_excluidos,
            adicionar_motivo_exclusao(linhas_descartadas_p1, "DUPLICADO - PRIMEIRA PALAVRA REPETIDA"),
        ],
        ignore_index=True,
    )

    return df_mantidos.reset_index(drop=True), df_excluidos


df = pd.read_excel(arquivo, sheet_name=0, dtype={"COD USUARIO": str})
df["COD USUARIO"] = df["COD USUARIO"].apply(normalizar_chave)

for coluna in COLUNAS_PRINCIPAIS:
    if coluna not in df.columns:
        df[coluna] = ""

df["IDADE T"] = (
    df["IDADE"].astype("string").str.extract(r"^\s*([^Aa]*)[Aa]", expand=False).str.strip().fillna("")
)

df, df_excluidos_parto_laqueadura = remover_parto_laqueadura(df)

colunas_extras = [coluna for coluna in df.columns if coluna not in COLUNAS_PRINCIPAIS]
df = df[COLUNAS_PRINCIPAIS + colunas_extras]

df, df_excluidos_duplicados = remover_duplicados_por_regras(df)
df, resumo_telefones = adicionar_telefones_por_senha(df, arquivo_telefones)
df = ordenar_por_data_internacao(df)
df = sinalizar_duplicidade(df)
df_excluidos = juntar_excluidos(
    [df_excluidos_parto_laqueadura, df_excluidos_duplicados],
    df.columns,
)

pasta_saida.mkdir(parents=True, exist_ok=True)
pasta_relatorios.mkdir(parents=True, exist_ok=True)

with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="BASE", index=False)

    for nome_aba, colunas in ABAS_AJUSTADAS.items():
        df_aba = pd.DataFrame(columns=colunas)
        df_aba.to_excel(writer, sheet_name=nome_aba, index=False)

aplicar_formulas(arquivo_saida)

arquivo_excluidos = pasta_relatorios / "linhas_excluidas.xlsx"
df_excluidos.to_excel(arquivo_excluidos, index=False)

print(f"Planilha organizada com sucesso em: {arquivo_saida}")
print(f"Linhas removidas com PARTO ou LAQUEADURA: {len(df_excluidos_parto_laqueadura)}")
print(f"Linhas removidas por duplicidade: {len(df_excluidos_duplicados)}")
print(f"Total de linhas removidas: {len(df_excluidos)}")
print(f"Arquivo unico com linhas removidas: {arquivo_excluidos}")

if resumo_telefones["executado"]:
    print(f"Total de SENHAS encontradas com telefone: {resumo_telefones['senhas_com_telefone']}")
    print(f"Match por SENHA: {resumo_telefones['match_senha']}")
    print(f"Match por COD USUARIO: {resumo_telefones['match_cod_usuario']}")
    print(f"Match encontrado, mas sem telefone: {resumo_telefones['match_sem_telefone']}")
else:
    print(resumo_telefones["motivo"])

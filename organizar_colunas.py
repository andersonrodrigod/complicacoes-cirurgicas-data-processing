import pandas as pd
from pathlib import Path


arquivo = Path("data/complicacao.xlsx")
pasta_saida = Path("data/complicacao_ajustada")
arquivo_saida = pasta_saida / "complicacao.xlsx"

colunas_principais = [
    "COD FILIAL",
    "FILIAL",
    "BASE",
    "SIGLA",
    "ESTADO",
    "SENHA",
    "COMPLICACAO",
    "OBITO",
    "COD USUARIO",
    "USUARIO",
    "TELEFONE",
    "IDADE",
    "IDADE T",
    "EMPRESA",
    "COD PLANO",
    "PLANO",
    "DT ADESAO",
    "TEMPO PLANO",
    "DIAS CARENCIA",
    "TP ATENDIMENTO",
    "TRATAMENTO",
    "PRESTADOR",
    "SOLICITANTE",
    "COD PROCEDIMENTO",
    "PROCEDIMENTO",
    "DT AUTORIZACAO",
    "DT INTERNACAO",
    "DT ENVIO",
    "DIARIAS",
    "UTI",
    "OBSERVACAO",
    "CHAVE",
    "OPERADOR",
    "CONTATO",
    "DT ENVIO MANUAL",
    "DATA DO CONTATO",
    "LIDA",
    "RESPOSTA",
    "STATUS",
    "DATA DE ENVIO",
    "P1",
    "P2",
    "P3",
    "P4",
    "OBSERVA\u00c7\u00d5ES DO CLIENTE",
    "RP1",
    "RP1 Nº",
    "TENTATIVA",
    "DATA ULTIMA TENTATIVA",
    "TELEFONE TENTADO",
    "ESPECIALISTA",
    "TIPO",
    "UF",
    "DISTRITO",
    "TELEFONE 1",
    "TELEFONE 2",
    "TELEFONE 3",
    "TELEFONE 4",
    "TELEFONE 5",
]

colunas_pesquisa = [
    "Data",
    "Nome",
    "Telefone",
    "CPF/CNPJ",
    "Resposta",
    "Opção",
    "Protocolo",
    "Cod.",
    "Número externo",
    "Agente",
    "Canal",
    "Conta",
    "Serviço",
    "UF",
    "Classificação",
    "Entrada",
    "Classificação de IA",
]

abas = {
    "STATUS": [
        "Conta",
        "HSM",
        "Mensagem",
        "Categoria",
        "Template",
        "Data do envio",
        "Status",
        "Respondido",
        "protocolo",
        "Agendamento",
        "Data agendamento",
        "Status agendamento",
        "Campanha",
        "Agente",
        "Contato",
        "Telefone",
        "ID_Mailing",
        "DT ENVIO",
        "RESPOSTA",
        "NOME_MANIPULADO",
    ],
    "P1": colunas_pesquisa,
    "P2": colunas_pesquisa,
    "P3": colunas_pesquisa,
    "P4": colunas_pesquisa,
    "CHAVE ERRO": [
        "CHAVE ERRADA",
        "P1",
        "P2",
        "P3",
        "P4",
        "CHAVE CERTA",
    ],
    "RESUMO": [],
}


def remover_parto_laqueadura(df):
    # Remove procedimentos de parto e laqueadura da base ajustada e separa as linhas excluidas para conferencia.
    procedimento = df["PROCEDIMENTO"].fillna("").astype(str).str.upper().str.strip()
    mascara_excluir = procedimento.str.contains("PARTO|LAQUEADURA", na=False)

    df_filtrado = df[~mascara_excluir].copy()
    df_excluidos = df[mascara_excluir].copy()

    return df_filtrado, df_excluidos


df = pd.read_excel(arquivo, sheet_name=0)

for coluna in colunas_principais:
    if coluna not in df.columns:
        df[coluna] = ""

df["IDADE T"] = (
    df["IDADE"].astype("string").str.extract(r"^\s*([^Aa]*)[Aa]", expand=False).str.strip().fillna("")
)

df, df_excluidos_parto_laqueadura = remover_parto_laqueadura(df)

colunas_extras = [coluna for coluna in df.columns if coluna not in colunas_principais]
df = df[colunas_principais + colunas_extras]

pasta_saida.mkdir(parents=True, exist_ok=True)

with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="BASE", index=False)

    for nome_aba, colunas in abas.items():
        df_aba = pd.DataFrame(columns=colunas)
        df_aba.to_excel(writer, sheet_name=nome_aba, index=False)

arquivo_excluidos = pasta_saida / "linhas_excluidas_parto_laqueadura.xlsx"
df_excluidos_parto_laqueadura.to_excel(arquivo_excluidos, index=False)

print(f"Planilha organizada com sucesso em: {arquivo_saida}")
print(f"Linhas removidas com PARTO ou LAQUEADURA: {len(df_excluidos_parto_laqueadura)}")
print(f"Arquivo com linhas removidas: {arquivo_excluidos}")

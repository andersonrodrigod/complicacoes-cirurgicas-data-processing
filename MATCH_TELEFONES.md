# Match de telefones

O processamento tenta enriquecer a base final com telefones usando o arquivo:

```text
data/telefone_abril_internacoes.csv
```

A regra principal deveria ser o match por senha:

```text
BASE.SENHA = CSV.CD_SENHA
```

No CSV atual, a coluna disponivel para senha vem como `CD_SENHA_AUTORIZA`. Em alguns casos ela nao corresponde a verdadeira `CD_SENHA` esperada pela base de complicacoes. Por isso, parte dos registros nao encontra telefone pelo match principal.

Para reduzir essa perda, o codigo faz um segundo match temporario:

```text
BASE.COD USUARIO = CSV.CD_USUARIO
```

Esse segundo match existe apenas como fallback enquanto a query SQL do CSV nao trouxer a senha correta.

Quando remover o fallback por COD USUARIO:

- ajustar a query SQL para retornar a verdadeira `CD_SENHA`;
- validar que o match por `SENHA` recupera os telefones esperados;
- remover do `processar_complicacao.py` o bloco marcado como match temporario por `COD USUARIO`;
- remover tambem a coluna/contador `MATCH_COD_USUARIO`, se nao for mais necessario para auditoria.

Resumo: o caminho ideal e manter somente o match por senha. O match por `COD USUARIO` existe para compensar a base atual de telefones.

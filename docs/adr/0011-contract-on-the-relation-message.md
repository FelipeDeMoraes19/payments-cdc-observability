# ADR 0011 — Contrato validado na mensagem `Relation`, e o que ele deliberadamente não valida

- Status: aceito
- Data: 2026-08-26

## Contexto

O Marco 1 exige que uma mudança de tipo na origem derrube o pipeline com mensagem clara,
antes de gravar qualquer linha do formato novo. Falta decidir **onde** o contrato é
verificado e **o que** ele cobre.

## Decisão

Duas camadas, e a primeira é a que importa.

**1. Forma, na mensagem `Relation`.** O `pgoutput` manda nome, OID de tipo e flag de chave
de cada coluna antes de qualquer linha daquela relação. O contrato compara isso com o que
declara. Coluna que sumiu, coluna que apareceu, tipo que mudou ou chave que mudou: falha
ali, com o LSN da transação, e nenhuma linha do formato novo chega ao bronze.

**2. Valor, com Pydantic**, por ação:

| Ação | O que é validado | Por quê |
|---|---|---|
| `insert` | o modelo inteiro | um insert traz todas as colunas; ausência é defeito |
| `update` | só as colunas presentes | TOAST não alterado chega ausente e é legítimo (ADR 0001) |
| `delete` | só as colunas de chave | `REPLICA IDENTITY DEFAULT` manda o resto como nulo |

Tabela publicada sem contrato falha alto: ou alguém adiciona o contrato, ou tira a tabela
da publication. Publicar sem contratar é o começo do dado que ninguém sabe de onde vem.

Os OIDs são declarados por **nome** (`numeric`, `timestamptz`) e resolvidos por um mapa
único. Um teste compara esse mapa com o `pg_type` do servidor, então o número mágico não
apodrece em silêncio.

## Alternativas rejeitadas

**Inferir o schema dos valores.** É o que quase toda ferramenta faz, e só detecta o drift
quando ele já produziu lixo — e mal: `numeric` virando `text` continua parecendo número no
valor `"199.90"`. A `Relation` diz `oid 1700` virou `oid 25` antes da primeira linha. É
exato, e chega mais cedo.

**Validar só a linha, com Pydantic, sem olhar a `Relation`.** Mais simples e mais fraco.
Uma coluna nova na origem passaria despercebida enquanto os tipos das antigas coubessem.

**`status` como enum no contrato.** Tentador, e errado aqui. Um status novo no negócio é
**deriva de dado**, não de schema, e derrubar a ingestão por causa dele transforma um fato
comercial em incidente. Isso é trabalho do `accepted_values` do dbt, no gold, onde falha
sem parar o fluxo. O contrato de bronze cuida da forma; o teste de dbt cuida do domínio.

**Gerar o contrato a partir do banco.** Um contrato derivado da origem concorda com a
origem por construção e nunca detecta nada. Contrato tem que ser declaração independente.

## Consequências

- O consumidor sai com código **2** e uma linha só, sem traceback:

```
contract violation: column public.payments.amount changed type at LSN 0/1AD3C68:
the contract expects numeric (oid 1700), the stream carries text (oid 25)
```

- Linhas pendentes no lote não são gravadas nem confirmadas quando o contrato falha, então
  o replay depois da correção é o comportamento normal do ADR 0008, sem caso especial.
- Coluna anulável ainda não é representável: todas as colunas são `NOT NULL` hoje e o
  contrato assume isso. Adicionar uma coluna anulável exige `Optional` no modelo.
- Adicionar tabela à publication passa a exigir contrato no mesmo commit. É atrito de
  propósito.

## Evidência

Duas coisas quebraram na cara, e nenhuma estava no plano:

**O Pydantic recusa o timestamp que o Postgres manda.** `2026-08-26 02:55:27.56529+00`
falha com *unexpected extra characters at the end of the input*: o offset de dois dígitos
não é ISO-8601 válido. Resolvido com um `BeforeValidator` que normaliza `+00` para
`+00:00`. Sem isso, todo registro com timestamp seria rejeitado — ou seja, todos.

**`FieldInfo.annotation` descarta o `Annotated`.** O `BeforeValidator` acima vive em
`field.metadata`, não em `field.annotation`. O validador por campo, usado em `update` e
`delete`, perdia a normalização e rejeitava exatamente os timestamps que o caminho de
`insert` aceitava. O sintoma era absurdo o suficiente para confundir: insert passa, update
falha, mesma coluna, mesmo valor. Corrigido remontando `Annotated[tipo, *metadata]`.

Os dois foram encontrados pela suíte, não por leitura. Os quatro testes do Marco 1 passam:

```
tests/test_restart_consistency.py::...loses_nothing_and_duplicates_are_exact  PASSED
tests/test_restart_consistency.py::...replayed_identically                    PASSED
tests/test_schema_contract.py::test_registered_type_oids_match_the_server      PASSED
tests/test_schema_contract.py::...fails_loudly_and_writes_nothing_new          PASSED
```

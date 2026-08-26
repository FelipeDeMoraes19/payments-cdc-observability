# ADR 0008 — Estado no slot e no dado, nunca em arquivo paralelo

- Status: aceito
- Data: 2026-08-25

## Contexto

As duas fontes do pipeline precisam lembrar onde pararam. O CDC precisa saber de que LSN
retomar; o extrator batch do BCB precisa saber a última data já baixada. A pergunta é
onde esse ponteiro mora.

## Decisão

**O ponteiro mora no mecanismo que já é durável, nunca num arquivo de estado à parte.**

- **CDC:** o checkpoint é o `confirmed_flush_lsn` do slot de replicação. O consumidor
  grava o bronze, espera o `os.replace` completar, e **só então** chama `send_feedback`.
  Reiniciar o consumidor retoma exatamente da última posição confirmada.
- **Batch BCB:** o watermark é derivado do próprio bronze — a maior data presente em
  `bronze/fx/`. Não existe arquivo de watermark.

## Alternativas rejeitadas

**`_state.json` ao lado dos dados.** É o padrão mais comum e cria duas fontes de verdade
que divergem exatamente quando importa: a falha no meio da escrita. Se o processo morre
entre gravar o Parquet e gravar o JSON de estado, o pipeline reprocessa ou pula, e não há
como saber qual dos dois arquivos está certo. Um checkpoint que pode discordar do dado
não é checkpoint.

**Tabela de controle no Postgres de origem.** Escrever estado do pipeline no OLTP acopla
consumidor e fonte, e transforma a origem em dependência de escrita de quem só deveria
ler. Também gera WAL, ou seja, o consumidor passa a alimentar o próprio stream.

**Confirmar o LSN antes de gravar o bronze.** Elimina a duplicata e cria perda de dado,
que é estritamente pior. At-least-once com dedup é recuperável; at-most-once não é.

## Consequências

- O consumidor não tem estado local. Apagar `data/` e reiniciar não confunde o pipeline:
  o slot continua sendo a verdade.
- Duplicata passa a ser um resultado **esperado** e não um defeito. A janela entre gravar
  e confirmar é real e deliberada, e é o que o `make chaos-replay` injeta (plano, seção 9).
  O dedup por `(pk, lsn)` do ADR 0002 é o par obrigatório desta decisão.
- Surge um custo operacional: slot que não avança retém WAL indefinidamente e enche o
  disco, em silêncio. Vira linha da tabela de modos de falha e `make chaos-orphan-slot`,
  no Marco 3, porque detectá-lo exige métrica e alerta no Grafana.
- O rerun do batch tem que ser idempotente por construção, já que o watermark vem do
  dado: mesmo intervalo gera o mesmo nome de arquivo e sobrescreve em vez de duplicar.

## Evidência

Medido em 2026-08-25, e é o que sustenta a decisão inteira.

O `confirmed_flush_lsn` **não avança sozinho**. Duas execuções seguidas do dump de
streaming, sem nenhuma chamada a `send_feedback`, mantiveram a posição em `0/1982C18`
nas duas. A segunda execução reentregou as seis mensagens da primeira, byte a byte e com
os mesmos LSNs, e ainda trouxe as novas. Ou seja: o consumidor controla o avanço, e
"gravar e depois confirmar" é implementável sem truque.

O slot também **não rebobina** — não é opinião, é o servidor recusando:

```
ERROR:  cannot advance replication slot to 0/1982798, minimum is 0/1982BA8
```

`pg_replication_slot_advance` só anda para frente. Isso encerra a ideia de reprocessar
pedindo um LSN anterior: a única fonte legítima de replay é a janela entre a gravação e
a confirmação.

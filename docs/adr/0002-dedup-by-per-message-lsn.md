# ADR 0002 — Dedup por LSN de mensagem, não por `updated_at` nem por LSN de commit

- Status: aceito
- Data: 2026-08-25

## Contexto

O consumidor CDC é at-least-once por construção: ele grava o bronze e só depois confirma
a posição ao Postgres. A janela entre as duas coisas é onde nasce a duplicata. Alguma
chave precisa dizer se dois registros no bronze são a mesma mudança ou duas mudanças
diferentes na mesma linha.

## Decisão

A chave de deduplicação é **`(pk, lsn)`**, onde `lsn` é a posição de WAL **da mensagem**,
lida do cabeçalho `XLogData` do protocolo de streaming (`data_start`).

Não é o LSN do commit da transação. Não é `updated_at`.

## Alternativas rejeitadas

**`updated_at` da origem.** É o candidato natural e está errado por três motivos.
Depende do relógio de quem escreve; duas mudanças dentro do mesmo milissegundo colidem;
e nada obriga a aplicação a atualizar a coluna. Uma linha que muda sem mexer no
`updated_at` fica invisível, e o pipeline não tem como saber.

**LSN do commit.** É a alternativa que parece equivalente e não é — foi por isso que
este ADR foi reescrito antes de existir código. Todas as mudanças de uma transação
compartilham o LSN de commit. Se a mesma PK muda mais de uma vez na mesma transação,
`(pk, lsn_do_commit)` colapsa versões distintas em uma só e o dedup **descarta dado
bom**. Não é hipótese: está medido abaixo.

**Hash da linha inteira.** Deduplica o replay, mas também deduplica uma mudança
legítima que devolve a linha a um valor anterior — `captured` volta para `pending` e o
registro some. Perde a ordem, que é justamente o que o LSN dá de graça.

## Consequências

- O parser tem que carregar o `data_start` do envelope `XLogData` para dentro de cada
  registro do bronze. O corpo da mensagem `pgoutput` no `proto_version 1` **não** contém
  o LSN da mudança; ele só existe no envelope. Perder o envelope é perder a chave.
- A mensagem `Relation` chega com `data_start = 0/0`. Ela é metadado, não mudança, e não
  entra no bronze — mas um consumidor que gravasse todas as mensagens indiscriminadamente
  produziria linhas com LSN zero. O parser trata `Relation` como estado de sessão.
- O LSN é monotônico, então ele serve de critério de ordenação para o silver: a versão
  vigente de uma PK é a de maior LSN. Dedup e ordenação usam o mesmo campo.
- Duplicata de replay é **exata**: mesma `(pk, lsn)`, mesmo conteúdo. Isso torna o
  critério de aceite do Marco 1 verificável — nunca duas versões diferentes com a mesma
  chave.

## Evidência

Medido em 2026-08-25. Uma transação (`xid 755`) com três `UPDATE` na mesma linha:

```
    lsn    | xid | msg
-----------+-----+-----
 0/1982970 | 755 | B
 0/1982970 | 755 | U
 0/19829F8 | 755 | U
 0/1982AF0 | 755 | U
 0/1982BA8 | 755 | C

 update_messages | distinct_lsns
-----------------+---------------
               3 |             3
```

Três mudanças, três LSNs distintos, um único LSN de commit (`0/1982BA8`). Com a chave de
commit, três versões da mesma PK viram uma.

O mesmo resultado pelo protocolo de streaming, que é o caminho que o consumidor usa:
duas atualizações na mesma transação chegaram com `data_start` `0/1982DC0` e `0/1982E48`
(`update messages: 2  distinct data_start: 2`).

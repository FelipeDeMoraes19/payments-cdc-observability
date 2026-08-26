# ADR 0016 — Semântica do silver: uma linha por chave, e o que acontece com o que morreu

- Status: aceito
- Data: 2026-08-26

## Contexto

O bronze é um log de mudanças: várias linhas por chave, cada uma com seu LSN, algumas com
colunas ausentes, algumas anunciando que a linha deixou de existir. O silver precisa
entregar **uma linha por chave**, tipada e mascarada, sem perder o que o log sabia.

## Decisão

**Versão vigente por `row_number()` sobre janela.**

```
row_number() over (partition by key order by lsn_numeric desc) = 1
```

**Coluna TOAST não alterada é preenchida com o último valor conhecido**, via
`last_value(coluna, ignoreNulls = true)` sobre a mesma janela ordenada por LSN crescente,
antes de escolher a versão vigente.

**`delete` e `truncate` viram estado, não ausência.** A linha permanece no silver com
`is_deleted = true` e o LSN em que morreu.

**Tipagem vem do contrato**, o mesmo `contracts/tables.py` que a ingestão usa. Não existe
segunda declaração do schema.

**Idempotência por `partitionOverwriteMode = dynamic`**: reprocessar uma partição
substitui exatamente aquela partição.

**Parse inválido falha alto, com a coluna nomeada.** O modo ANSI fica **fixado
explicitamente** na sessão, e antes de tipar qualquer coluna o job verifica, por coluna,
se algum valor não nulo vira nulo sob `try_cast`. Se virar, ele levanta nomeando **coluna,
valor observado e tipo esperado**.

**O parse mora no silver, nunca no consumidor CDC.** Bronze é o que chegou; silver é o que
aquilo significa. Bronze não rejeita.

## Alternativas rejeitadas

**`dropDuplicates(["key"])`.** Uma linha, e **errada**. Não garante qual das versões
sobrevive: sem ordenação explícita, o vencedor é o que o shuffle entregar primeiro, o que
varia entre execuções. Produz um silver que muda sozinho sem nada ter mudado na origem — e
quebra o critério de aceite do Marco 2, que exige rodar duas vezes e obter o mesmo gold.

**`groupBy(key).agg(max(lsn))` com join de volta.** Correto e mais caro: agrega num
shuffle e reencontra a linha inteira noutro. A janela carrega a linha completa e resolve
com um. Em milhares de linhas a diferença é irrelevante; a escolha é sobre qual código
continua certo quando o volume for outro.

**Ignorar o marcador de TOAST não alterado.** É o que um job ingênuo faz, e corrompe em
silêncio: a coluna ausente vira nulo, e o `legal_name` de qualquer comerciante que sofra um
`UPDATE` some do silver. O bronze registrou o marcador exatamente para que isto não
acontecesse (ADR 0001); ignorá-lo desperdiçaria a informação já capturada.

**Apagar a linha do silver quando chega um `delete`.** Some com a evidência. O SCD2 da
`dim_customer` precisa saber **quando** a linha morreu para fechar o intervalo de validade;
uma linha ausente não tem data de morte. E um `delete` seguido de reinserção da mesma
chave ficaria indistinguível de um registro que nunca sumiu.

**Interpretar `truncate` no gold em vez do silver.** O evento de truncate não tem chave
(ADR 0012), então quem o aplica precisa conhecer o LSN e a tabela para invalidar tudo o que
veio antes. Esse é raciocínio de linhagem de mudanças, que é o trabalho do silver.

## Consequências

- Uma linha vigente por chave, sempre, mesmo que ela esteja morta. Quem consome escolhe se
  filtra `is_deleted`.
- `truncate` invalida por comparação de LSN: toda chave da tabela cuja última mudança tem
  LSN **anterior** ao do truncate passa a `is_deleted`. Isso fecha a dívida que o ADR 0012
  deixou explicitamente em aberto.
- A ordenação depende de `lsn_numeric`, não de `lsn` em texto. O ADR 0013 mediu por que:
  como texto, `0/9FFFFFF` ordena depois de `0/10000000` e a versão vigente sai errada.
- O contrato vira dependência de execução do job, e não só da ingestão. Mudar uma coluna
  passa a exigir tocar um lugar só, que é o ponto.
- `partitionOverwriteMode = dynamic` só é idempotente se a partição for função determinística
  do dado. Aqui é: a data de commit do evento.

## Evidência: o Spark 4.2.0 já falha alto, e mesmo assim a checagem fica

O medo era o clássico: `to_timestamp` em modo permissivo devolve `NULL` no parse inválido e
produz silver com buraco silencioso — exatamente o modo de falha que este projeto existe
para denunciar, acontecendo dentro dele. Medido na 4.2.0, dentro do container:

```
spark.sql.ansi.enabled            = true
spark.sql.legacy.timeParserPolicy = CORRECTED
spark.sql.storeAssignmentPolicy   = ANSI

'2026-08-26 02:55:27.56529+00'  to_timestamp     -> 2026-08-26 02:55:27.565290
'not-a-timestamp'               to_timestamp     -> RAISED DateTimeException [CAST_INVALID_INPUT]
'not-a-timestamp'               try_to_timestamp -> None
```

Duas conclusões, e nenhuma torna a checagem dispensável.

O **modo ANSI é padrão no Spark 4**, então o comportamento correto já vem de fábrica e o
permissivo virou opt-in (`try_to_timestamp`). Isso muda em relação à linha 3.x, onde o
padrão devolvia nulo. Ainda assim, `spark.sql.ansi.enabled` fica **fixado na sessão**:
depender de um padrão é depender de uma decisão que outra pessoa pode reverter num
`spark-submit --conf`, e o comportamento de falha do pipeline não pode ser herdado por
acaso.

A mensagem nativa do Spark nomeia o valor e o tipo alvo, mas **não nomeia a coluna** —
diz `The value 'not-a-timestamp' ... cannot be cast to "TIMESTAMP"` sem dizer onde. Num
silver com dezenas de colunas isso é uma caça ao tesouro. Por isso a checagem por coluna
com `try_cast` continua: ela entrega a mensagem que o critério de aceite exige.

**E ela custa uma passada a mais, o que só é irrelevante porque este volume é minúsculo.**
Em escala real a conta muda: uma varredura adicional por coluna tipada, sobre um dataset
que não cabe em memória, é caro o bastante para a decisão ser outra — amostrar em vez de
varrer tudo, ou aceitar a mensagem crua do Spark e pagar o custo de diagnóstico só quando
a falha acontece. Registrar isso é a mesma honestidade do ADR 0015 sobre o Spark estar
sobredimensionado aqui: a escolha é defensável **neste** contexto, e dizer qual é o
contexto é parte da escolha.

Nota lateral que vale registrar: o offset de **dois dígitos** do Postgres
(`+00`), que obrigou um `BeforeValidator` no contrato do Pydantic (ADR 0011), o Spark
parseia sem ajuste nenhum. Dois parsers, duas tolerâncias diferentes para o mesmo dado.

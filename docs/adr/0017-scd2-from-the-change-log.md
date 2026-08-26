# ADR 0017 — SCD Type 2 construído do log de mudanças, e não com `dbt snapshot`

- Status: aceito
- Data: 2026-08-26

## Contexto

O plano, seção 5, diz `dim_customer` em SCD2 "implementado com `dbt snapshot`". Essa é a
resposta convencional, e é a resposta certa para quem **não tem** histórico: o `snapshot`
fotografa o estado da origem a cada execução e infere a mudança comparando fotos.

Este projeto tem o log de mudanças completo e ordenado por LSN. A pergunta é se a
ferramenta convencional ainda serve quando a premissa dela deixou de valer.

## Decisão

**A `dim_customer` é construída a partir do log de mudanças do silver**, com modelos dbt
comuns. `dbt snapshot` não é usado.

`valid_from` vem do **tempo de commit** da mudança. `valid_to` é o `valid_from` da versão
seguinte, e é nulo na versão vigente. Versões da **mesma transação são colapsadas**,
mantendo a última.

## O que `dbt snapshot` realmente faz, antes de rejeitá-lo

Rejeição informada e desconhecimento produzem o mesmo texto se o texto for vago. Então,
verificado na fonte do dbt e não de memória:

```sql
-- strategies.sql
snapshot_timestamp_strategy:  {% set updated_at = config['updated_at'] %}
snapshot_check_strategy:      {% set updated_at = config.get('updated_at', snapshot_get_time()) %}

-- helpers.sql
{{ strategy.updated_at }} as dbt_valid_from
```

Ou seja: sob a estratégia **`timestamp`**, o `dbt_valid_from` sai da coluna da origem e é
**derivado do dado**. Reexecutar sem dado novo não muda nada. Sob **`check`**, o padrão é
`snapshot_get_time()` — o relógio da execução — mas até isso é configurável.

**Uma versão anterior deste ADR afirmava que o `snapshot` carimba o relógio da execução, e
isso está errado como afirmação geral.** Fica registrado porque o erro é instrutivo: a
crítica preguiçosa à ferramenta é factualmente falsa, e um ADR que a repetisse perderia a
credibilidade inteira mesmo chegando à conclusão certa.

## Alternativas rejeitadas

**`dbt snapshot`, em qualquer estratégia.** O argumento que sobrevive às duas não é o
carimbo, é o **momento da observação**:

> O `snapshot` só enxerga o estado que existia **no instante em que ele rodou**.

Uma mudança que nasce e morre entre duas execuções nunca existiu para ele. Um pagamento
que vai de `pending` para `authorized` e para `captured` em noventa segundos, com o job
rodando de hora em hora, aparece como uma transição só — e as outras duas desapareceram
sem deixar rastro nem aviso. Nenhuma estratégia recupera isso, porque o dado não foi
perdido no processamento: ele **nunca foi observado**. A fidelidade do SCD2 passa a ser
função da frequência do agendador, o que é uma propriedade operacional vazando para dentro
da semântica do dado.

Disso decorre o segundo problema: **backfill é impossível de expressar.** O `snapshot` não
tem noção de "como estava no dia 12"; ele só sabe *agora*. Não existe reprocessar um dia.
O critério de aceite do Marco 2 — backfill de sete dias dando o mesmo resultado que rodar
dia a dia — não é difícil com `snapshot`, é **sem sentido**, porque não há como
reconstruir história que nunca foi vista.

Detalhe menor e verificado: no fechamento de linha apagada, o `dbt_valid_to` recebe
`snapshot_get_time()` **em qualquer estratégia**. Mesmo sob `timestamp`, a hora da morte é
a hora do job, não a hora em que a linha morreu.

**O gold ler o bronze direto**, pulando o silver. Desqualificada por uma razão que encerra
sozinha: **o mascaramento de PII acontece no silver** (ADR 0005). Gold lendo bronze veria
CPF em claro.

## O que se perde, dito com honestidade

**Durabilidade contra perda da origem.** A tabela do `snapshot` sobrevive mesmo se o
histórico da origem for podado; a dimensão construída do log só existe enquanto o log
existir. Aqui o bronze nunca é podado — e se fosse, a história de replay do projeto
inteiro já estaria quebrada antes desta decisão importar.

**Familiaridade.** `dbt snapshot` é o que um leitor espera encontrar. Não usá-lo exige
este documento; sem ele, a escolha lê como desconhecimento da ferramenta.

**Código próprio.** O intervalo de validade passa a ser responsabilidade nossa — janela,
`lead()`, fechamento na versão vigente e no delete. São mais linhas e mais superfície para
errar, em troca de exatidão e de reprocessamento determinístico.

## A decisão de borda: versões da mesma transação

`valid_from` vem do tempo de commit, porque o LSN ordena mas não significa nada para o
negócio. Só que duas mudanças na mesma transação **compartilham o tempo de commit**, o que
produziria intervalo de validade de duração zero.

Elas são colapsadas, mantendo a última:

> Guardar uma versão que ninguém pôde observar é inventar história.

Nenhum leitor jamais viu o estado intermediário, porque a transação não havia commitado. O
LSN continua sendo o critério de ordenação e de desempate.

**Isto não contradiz o ADR 0002**, e a confusão é fácil o bastante para merecer o
parágrafo. São camadas diferentes fazendo trabalhos diferentes:

| Camada | Chave | Para quê |
|---|---|---|
| silver | `(pk, lsn)` | matar duplicata de **replay**: a mesma mudança entregue duas vezes |
| gold | colapso por transação | não fabricar intervalo de validade de **duração zero** |

A primeira remove o que o transporte duplicou. A segunda remove o que nunca foi
observável. Uma não desfaz a outra; elas nem olham para o mesmo problema.

## Consequências

- A `dim_customer` é função determinística do silver. Rodar duas vezes dá o mesmo
  resultado, e reprocessar um intervalo dá o mesmo que ter rodado dia a dia — que é
  exatamente o critério de aceite do Marco 2, obtido por construção e não por cuidado.
- Nenhum modelo do gold pode usar relógio de execução. `current_timestamp` no gold é
  defeito, não estilo.
- O silver precisa carregar o **histórico completo**, não o estado vigente. Isso emenda o
  ADR 0016, que resolvia isso na camada errada.

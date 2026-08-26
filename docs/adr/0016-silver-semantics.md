# ADR 0016 — Semântica do silver: uma linha por chave, e o que acontece com o que morreu

- Status: aceito
- Data: 2026-08-26

## Contexto

O bronze é um log de mudanças: várias linhas por chave, cada uma com seu LSN, algumas com
colunas ausentes, algumas anunciando que a linha deixou de existir. O silver precisa
entregar **uma linha por chave**, tipada e mascarada, sem perder o que o log sabia.

## Decisão

**O silver guarda o histórico limpo completo**, uma linha por `(chave, lsn)`, com a versão
vigente marcada por `is_current`:

```
is_current = row_number() over (partition by key order by lsn_numeric desc) = 1
```

> **Emenda de 2026-08-26.** A decisão original era "uma linha por chave", colapsando o
> histórico aqui. Estava errada, e o erro é de camada. A deduplicação que o silver deve
> fazer é a de **replay**, por `(pk, lsn)` — a mesma mudança entregue duas vezes. Colapsar
> versões distintas de uma chave não é limpeza, é **modelagem**, e modelagem é trabalho do
> gold, onde se decide entre SCD1 e SCD2. Um silver que já colapsou destrói a informação de
> que o SCD2 precisa, e o ADR 0017 mostra que reconstruí-la depois é impossível.
> A limpeza fica aqui; a escolha de forma fica no gold.

**Coluna TOAST não alterada é preenchida com o último valor conhecido**, via
`last_value(coluna, ignoreNulls = true)` sobre a mesma janela ordenada por LSN crescente,
antes de escolher a versão vigente.

**`delete` e `truncate` viram estado, não ausência.** A linha permanece no silver com
`is_deleted = true` e o LSN em que morreu.

**Tipagem vem do contrato**, o mesmo `contracts/tables.py` que a ingestão usa. Não existe
segunda declaração do schema.

**Idempotência por sobrescrita total** de cada tabela do silver. Mesma entrada, mesma
saída, sem estado sobrevivente entre execuções.

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

**`partitionOverwriteMode = dynamic`.** Foi a decisão original deste ADR, e estava errada.
Vale registrar o princípio e não só o conserto:

> **Sobrescrita dinâmica só é idempotente quando a partição é estável para a chave.**

O silver é um *snapshot* — uma linha por chave, com o estado vigente. Se a partição for a
data de commit da última mudança, então a partição de uma chave **muda exatamente quando a
chave muda**. A sobrescrita dinâmica não toca partição que não veio no lote, então a versão
velha continua viva na partição antiga e a chave passa a existir duas vezes. O layout
desfaz a deduplicação que o job acabou de fazer, e o critério de aceite do Marco 2 —
rodar duas vezes e obter o mesmo resultado — quebra sem que nada na origem tenha mudado.

Sobrescrita dinâmica é a ferramenta certa para tabela de **eventos**, onde a partição é o
tempo do evento e nunca se move. Errada para tabela de **estado**.

**Formato de tabela com `MERGE`** — Delta, Iceberg ou Hudi. É o que a resposta em escala
seria: fazer *upsert* por chave em vez de reescrever tudo, com a partição deixando de ser o
mecanismo de idempotência. Fica fora **por decisão de escopo, não por desconhecimento**: o
plano proíbe infraestrutura acessória (seção 2), e num volume de milhares de linhas a
sobrescrita total custa menos que a complexidade de manter um formato transacional. Em
volume real a conta inverte, e a decisão inverte junto.

**Interpretar `truncate` no gold em vez do silver.** O evento de truncate não tem chave
(ADR 0012), então quem o aplica precisa conhecer o LSN e a tabela para invalidar tudo o que
veio antes. Esse é raciocínio de linhagem de mudanças, que é o trabalho do silver.

## Consequências

- Toda versão de toda chave, sempre, inclusive as mortas. Quem quer o estado vigente
  filtra `is_current`; quem quer história tem ela inteira. Filtrar é responsabilidade de
  quem consome, e o nome da coluna existe para que esquecer disso seja difícil.
- `truncate` invalida por comparação de LSN: toda chave da tabela cuja última mudança tem
  LSN **anterior** ao do truncate passa a `is_deleted`. Isso fecha a dívida que o ADR 0012
  deixou explicitamente em aberto.
- A ordenação depende de `lsn_numeric`, não de `lsn` em texto. O ADR 0013 mediu por que:
  como texto, `0/9FFFFFF` ordena depois de `0/10000000` e a versão vigente sai errada.
- O contrato vira dependência de execução do job, e não só da ingestão. Mudar uma coluna
  passa a exigir tocar um lugar só, que é o ponto.
- Sobrescrita total significa reescrever o silver inteiro a cada execução. Neste volume é
  irrelevante; ver abaixo o que mudaria em escala.
- **O silver deixa de ser limitado pelo número de chaves e passa a crescer com o número de
  mudanças.** Uma linha que muda mil vezes ocupa mil linhas. Aqui é irrelevante, e é
  exatamente o tipo de coisa que morde em escala: o custo passa a acompanhar a taxa de
  alteração da origem, não o tamanho dela.
- **A sobrescrita total agora significa reprocessar todo o histórico a cada execução.**
  Continua idempotente e continua barato neste volume. Em escala a resposta seria
  **incremental por intervalo de LSN** — processar apenas o que chegou desde o último
  corte, anexando ao histórico já escrito. Não é feito aqui por escolha consciente: o
  incremental troca uma releitura barata por estado de controle entre execuções, que é
  precisamente o que o ADR 0008 evita enquanto puder.

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

## Nota: o HMAC roda em UDF Python, sem otimização Arrow

O mascaramento do ADR 0005 é HMAC-SHA256, e o Spark não tem função nativa de HMAC — só
`sha2`, `md5` e afins. Construir HMAC à mão em SQL, com `opad` e `ipad`, seria possível e
ilegível; a escolha é uma UDF Python, seguindo "legibilidade vence esperteza".

A UDF cai no **caminho não otimizado por Arrow**, e o Spark avisa: `Arrow optimization
failed to enable because PyArrow or Pandas is not installed`. Isso significa serialização
linha a linha entre a JVM e o Python, que é a parte cara de uma UDF.

`pyarrow` **não** entra na imagem por causa disso. A imagem instala o que o job importa e
nada mais (ADR 0015), e adicionar dependência para uma otimização que este volume não
precisa contraria a mesma disciplina. Em escala a decisão seria outra, e há dois caminhos:
instalar `pyarrow` e usar UDF vetorizada, que reduz o custo de serialização mas mantém o
Python no caminho; ou implementar o HMAC como expressão nativa e nunca sair da JVM, que é
mais rápido e mais difícil de ler. Qual dos dois depende de quanto o perfil de execução
mostrar que a UDF pesa — e neste projeto ela não pesa nada.

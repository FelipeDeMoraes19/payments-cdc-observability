# ADR 0001 — Consumidor CDC próprio, lendo `pgoutput`

- Status: aceito
- Data: 2026-08-25

## Contexto

A ingestão do OLTP é por CDC via replicação lógica do Postgres. Existem duas escolhas
acopladas: **quem lê o WAL** (ferramenta pronta ou código próprio) e **em que formato o
Postgres entrega a mudança** (plugin de saída).

O projeto existe para mostrar as partes chatas de uma plataforma de dados. Slot, LSN,
replay e idempotência são a parte chata da ingestão. Uma ferramenta pronta faz a
ingestão funcionar e faz o aprendizado desaparecer.

## Decisão

Consumidor próprio em Python, lendo o protocolo de replicação de streaming
(`START_REPLICATION`) com o plugin **`pgoutput`**, `proto_version 1`, valores em texto.

Cliente: **psycopg2**. Não é preferência — é a única opção. Medido em 2026-08-25:
psycopg2 2.9.11 expõe `LogicalReplicationConnection`, `ReplicationCursor.start_replication`
e `send_feedback`; psycopg 3.2.12 não tem módulo nem símbolo de replicação. A biblioteca
foi escolhida pela restrição, e o resto do repositório usa a mesma para não conviver com
dois clientes de Postgres.

## Alternativas rejeitadas

**Debezium.** É a resposta certa em produção e a errada aqui. Traz Kafka Connect junto,
o que colide com o não-objetivo "sem Kafka" (plano, seção 2), e resolve exatamente o que
este repositório quer demonstrar que sabe resolver.

**Airbyte.** Mesmo problema, com menos controle sobre o LSN. O conector expõe um botão,
não um slot.

**`wal2json`.** Tecnicamente a alternativa mais próxima: entrega JSON e dispensa o parser
binário. Rejeitada por dois motivos. É extensão, e não vem na imagem oficial do Postgres —
exigiria Dockerfile próprio ou imagem de terceiro, o que encarece o `docker compose up`.
E entrega pronto justamente o artefato que dá valor ao repositório. Continua sendo o
**plano B**, ver condição de aborto.

**`test_decoding`.** Está no core e é texto, mas o próprio nome diz para que serve. O
formato é declaradamente instável entre versões e o escape de valores é ambíguo de
parsear. Não sustenta um ADR.

## Consequências

Aceito escrever e manter um parser binário de `pgoutput` — mensagens `Begin`, `Relation`,
`Insert`, `Update`, `Delete`, `Commit`, mais keepalive. Em troca:

- A mensagem `Relation` carrega nome e OID de cada coluna. O drift de schema aparece no
  próprio stream, antes de qualquer linha, o que dá ao contrato Pydantic uma falha exata
  em vez de uma inferência a partir de valores.
- O parser fica preso ao `proto_version 1`. Subir de versão do Postgres exige reler o
  documento de formato de mensagem antes de subir a imagem.

Duas armadilhas do formato ficam registradas porque quebram parser ingênuo em silêncio:
valor TOAST não alterado chega como marcador `u` e não como o valor (gravá-lo como NULL
corrompe o bronze); e a mensagem `Relation` chega com `data_start = 0/0`, sem posição de
WAL, porque é metadado e não mudança. Ambas foram observadas no spike, não presumidas.

## Condição de aborto

Se o parser passar de **dois dias de trabalho**, cai para `wal2json` e este ADR é
atualizado registrando a troca. A queda é decisão de custo tomada com antecedência, não
fracasso: o resto do consumidor — slot, feedback, micro-batch, idempotência — não muda,
só a função que transforma bytes em dicionário.

## Evidência

Spike de 2026-08-25, Postgres 16 em container, consumidor no host Windows.

- `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4` aplicados via
  `command` do Compose e confirmados em `pg_settings`.
- Slot `pgoutput` criado e decodificado com sucesso pelas duas vias: função SQL
  (`pg_logical_slot_peek_binary_changes`) e protocolo de streaming.
- Mensagens `B`, `R`, `I`, `U`, `D`, `C` observadas com os tamanhos e o byte de tipo
  esperados.
- `REPLICA IDENTITY DEFAULT` confirmado no comportamento: o `Delete` carrega só a PK
  (`K` seguido da chave e seis `n`), e o `Update` de PK inalterada não traz tupla antiga.
- psycopg2 no host Windows falou o protocolo de streaming sem ajuste. O plano B de rodar
  o consumidor dentro de container **não foi necessário**.

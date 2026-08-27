# ADR 0020 — O que é detectado por alerta e o que é detectado por teste

- Status: aceito
- Data: 2026-08-27

## Contexto

O Marco 3 traz observabilidade e a suíte de caos. A leitura preguiçosa seria instrumentar
tudo, exportar tudo e criar um alerta para cada coisa. Antes disso vale perguntar **o que
cada cenário de falha realmente precisa** para ser detectado.

## Decisão

Mapeando os oito cenários, **apenas três precisam de alerta**:

| Detectado por alerta | Detectado por teste |
|---|---|
| `chaos-empty` — heartbeat | `chaos-replay` e `chaos-schema` — já provados por pytest |
| `chaos-orphan-slot` — WAL retido | `chaos-fx-gap` — `not_null` em `amount_brl` |
| `chaos-blind` — por design, nunca | `chaos-pii` — teste customizado de padrão de CPF |
| | `chaos-late` — teste de freshness do dbt |

Cinco dos oito não passam por Prometheus nem por Grafana. Isso não é economia: é o
reconhecimento de que **alerta e teste respondem perguntas diferentes**. Teste pergunta
"este dado está certo?" e roda quando alguém manda. Alerta pergunta "este sistema está
vivo?" e roda sozinho. Usar alerta para o que um teste responde melhor produz alerta que
ninguém confia; usar teste para o que só um alerta pega produz silêncio às três da manhã.

### As métricas: duas, e cada uma sustenta um cenário

`cdc_records_written_total{table}` — contador. É o que o alerta de heartbeat lê, e é o alvo
que o alerta cego consulta com um rótulo que não existe.

`cdc_confirmed_lsn` — gauge. É o que mostra um slot que parou de avançar enquanto o WAL
cresce, que é a história do slot órfão.

Mais o `up` que o próprio Prometheus produz de graça, e que distingue **consumidor morto**
de **origem parada** — dois modos de falha que o contador sozinho confunde.

### Nada mede duração

O plano, seção 8, pede duração de cada etapa. Não vira métrica Prometheus, e o motivo é
estrutural: o consumidor é processo longo e pode ser raspado, mas **Spark e dbt são batch e
morrem antes da raspagem**. Exportar a duração deles exigiria Pushgateway — um serviço a
mais para alimentar um número que **nenhum dos três alertas consulta**.

E a duração já é medida por quem tem que medi-la: o Airflow registra duração de tarefa
nativamente, e o dbt registra por modelo no `run_results.json`. O orquestrador já mede;
duplicar seria inventar uma segunda fonte de verdade para um número que ninguém disputa.

### O WAL retido vem do Grafana consultando o Postgres

O `chaos-orphan-slot` mata o consumidor, e quem morreu não exporta métrica. A leitura tem
que vir de fora do processo. O Grafana consulta `pg_replication_slots` por SQL — verificado
na documentação que regras gerenciadas pelo Grafana podem consultar fonte SQL.

O papel `grafana` precisa de menos privilégio do que eu supunha. Medido em
**PostgreSQL 16.15**:

```
papel so com LOGIN e CONNECT  ->  payments_cdc | f | t | 1385776
o mesmo papel com pg_monitor  ->  payments_cdc | 1355 kB
```

`pg_monitor` é desnecessário. O papel não recebe `SELECT` em tabela nenhuma — ele **não
consegue ler `payments`, `customers` nem `merchants`**. A versão fica registrada porque as
permissões de leitura de `pg_replication_slots` e das funções de WAL mudaram entre versões
do Postgres, e esta medição vale para a 16.15.

Alternativa rejeitada: **`postgres_exporter`**. Um serviço a mais no `docker compose up`
para traduzir uma consulta SQL em métrica que só um alerta lê.

### A guarda de vacuidade, e o que ela não cobre

Um teste percorre as regras de alerta e afirma que a consulta de cada uma **retorna série
não vazia** — isto é, que o alerta é capaz de disparar. A lista de exceção contém
exatamente uma entrada, o alerta cego do ADR 0006.

Isso muda a natureza do alerta cego: o repositório deixa de só exibir um alerta cego e
passa a exibir **o mecanismo que encontra alertas cegos**, com a exceção codificada em vez
de confiada.

**O limite precisa estar escrito, aqui e na tabela do README:** a guarda detecta cegueira
por **série vazia**, não por **limiar inalcançável**. Um alerta com consulta saudável e
limiar `> 10^12` passaria por ela e seria igualmente cego. São dois modos de cegueira e ela
cobre um.

Há simetria nisso, e ela foi deliberada: o alerta cego usa exatamente o mecanismo que a
guarda detecta, e por isso precisa da exceção explícita. Se ele tivesse sido construído com
limiar absurdo, a guarda passaria calada e o repositório teria uma guarda incapaz de pegar
o próprio exemplo.

## Alternativas rejeitadas

**Instrumentar duração, contagem e erro em cada etapa.** É a bateria genérica que parece
completude e não sustenta cenário nenhum. Métrica que nenhum alerta consulta é custo de
manutenção disfarçado de rigor.

**Alertmanager e canal de notificação.** O critério de aceite é que cada comando de caos
produza o efeito documentado, e isso se verifica na regra e na consulta. Entregar
notificação é operação, não demonstração.

**`prometheus_client` direto em vez de OpenTelemetry.** Mais simples, menos peça, e
contraria a stack declarada no plano. O OTel entra porque o plano o declara e porque é o
que se espera saber defender.

## Consequências

- **O exportador Prometheus do OTel é beta.** Medido: `opentelemetry-sdk` está em 1.44.0 e
  classificado como estável; `opentelemetry-exporter-prometheus` está em **0.65b0**, ainda
  pré-1.0. O SDK é maduro; a ponte para o Prometheus não é.

  **Plano de saída, decidido agora e não na urgência:** se a ponte quebrar numa atualização,
  a troca é para **`prometheus_client` direto**. As duas métricas são um contador e um
  gauge, o exportador do OTel já escreve no registro do `prometheus_client` por baixo, e o
  nome das séries não muda. É uma troca de dependência, não de desenho, e está escrita aqui
  para ser executada como decisão em vez de conserto às pressas.

- O `docker compose up` passa a subir o consumidor, que segura o slot continuamente. Quem
  clona precisa saber disso, e vai para o README.

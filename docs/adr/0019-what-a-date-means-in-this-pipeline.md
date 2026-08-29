# ADR 0019 — O que uma data significa neste pipeline, e por que são duas DAGs

- Status: aceito
- Data: 2026-08-26

## Contexto

O Marco 2 termina com orquestração em Airflow. A tentação é desenhar uma DAG particionada
por data, porque é assim que quase todo tutorial começa. Antes disso é preciso responder o
que uma data **significa** aqui — e a resposta é diferente nas duas metades do pipeline.

**O extrator do BCB tem data de verdade.** Uma cotação pertence a um dia. "Rodar para o dia
12" é uma frase com referente: buscar o dia 12. Backfill é rebuscar um intervalo.

**O CDC não tem data. Tem posição.** Não se pede ao Postgres "as mudanças do dia 12"; pede-se
"o que houver depois do LSN N". O `commit_time` é **atributo** da mudança, não seletor por
onde se busca. E o slot não rebobina — o ADR 0008 mediu o servidor recusando:
`cannot advance replication slot to 0/1982798, minimum is 0/1982BA8`.

Para a metade CDC, "backfill do dia 12" não é difícil. É uma frase sem referente.

## Decisão

**Duas DAGs, porque as duas fontes têm noções de tempo diferentes.**

| DAG | Tempo | `catchup` | Backfill |
|---|---|---|---|
| `fx_daily` | data do pregão; cada execução busca o seu próprio intervalo | `False` | comando explícito, `airflow backfill` |
| `cdc_to_gold` | posição no WAL; drena o slot, roda silver e dbt | `False` | não existe: reprocessar é reconstruir do bronze |

**Sem acoplamento entre elas.** `cdc_to_gold` não espera pela cotação.

**O critério de aceite do Marco 2 fica emendado** — ver seção própria abaixo.

## Alternativas rejeitadas

**Uma DAG só, particionada por data.** O `data_interval` do Airflow é a promessa de que a
execução corresponde a uma fatia de tempo. Numa DAG única essa promessa seria verdadeira
para a tarefa do BCB e mentira para a do CDC. A mentira é operacional, não estética:
alguém faria *clear* de uma execução antiga esperando reprocessar aquele dia, e o CDC
drenaria o presente. `data_interval` é contrato, não enfeite.

**`catchup=True` na `fx_daily`.** Foi a proposta original deste ADR, corrigida pela
pesquisa. A documentação da linha 3.x é explícita:

> "By default, Dag runs that have not been run since the last data interval are not created
> by the scheduler upon activation of a Dag (Airflow config `scheduler.catchup_by_default=False`)."

Na 3.x o backfill deixou de ser efeito colateral do catchup e virou operação de primeira
classe, por UI ou CLI. `catchup=True` faria a DAG despejar meses de execuções no instante
em que alguém a ativasse — isso é surpresa, não backfill. E backfill explícito é **um
comando que se demonstra**, em vez de um efeito colateral que se explica.

**Acoplar as DAGs por Asset** (o que se chamava Dataset, renomeado no AIP-74/75). É a forma
idiomática de acoplamento na 3.x, e é por isso que ela merece rejeição explícita em vez de
silêncio. Acoplar faria o gold **esperar** pela cotação, e isso troca uma falha visível por
uma invisível: em vez de `amount_brl` nulo com o teste `not_null` apontando exatamente o
buraco, haveria uma DAG parada — e "não rodou" é muito mais difícil de diagnosticar que
"rodou e acusou".

Pior: **dia sem cotação é normal.** Fim de semana e feriado não têm PTAX (ADR 0014). Um
acoplamento que espera pela cotação esperaria para sempre todo sábado. O detector correto
já existe, e é o `not_null` em `amount_brl`.

**Silver incremental por data.** Resolveria o parâmetro de data de ponta a ponta e esbarra
numa propriedade global: `is_current` não é diário. Uma mudança que commita no dia 12
invalida o `is_current` de uma linha cuja última mudança foi no dia 3 — que está numa
partição que a execução do dia 12 não iria reescrever. O mesmo vale para o preenchimento de
coluna TOAST, que busca a última versão que tinha o valor, possivelmente semanas atrás.
Processar "só o dia 12" corretamente exigiria reescrever partições antigas, que é **merge**,
não particionamento. O ADR 0016 já registrou merge como a resposta em escala e o descartou
por escopo.

O perigo específico: um incremental por data que **não** reescreva as partições antigas
produz silver com duas linhas `is_current` para a mesma chave, e passa em qualquer teste
que só olhe a partição do dia.

## A emenda do critério de aceite

O plano dizia: *"backfill de 7 dias dar o mesmo resultado que rodar dia a dia"*. A frase foi
escrita antes de a arquitetura existir, e sobre esta arquitetura ela teria virado
**vacuidade**: com o silver e o gold reconstruindo tudo, o teste passaria sempre, inclusive
com o pipeline quebrado, porque nada a jusante depende do parâmetro de data.

A pergunta de fundo do backfill não é sobre datas — é sobre **composicionalidade**:
processar um intervalo dá o mesmo que processar as partes? Ela se mantém; o que muda é a
unidade em que cada metade sabe medi-la.

| Metade | Unidade | Critério |
|---|---|---|
| BCB | data | extrair 01–07 numa execução dá as mesmas observações que sete execuções de um dia |
| CDC | lote | drenar o mesmo conjunto de mudanças em muitos lotes pequenos dá o mesmo bronze, silver e gold que drenar num lote só |

Nenhum dos dois passa por acidente. O do BCB exercita a janela mensal, a reescrita de mês
inteiro e o watermark do ADR 0014. O do CDC exercita o eixo em que o tamanho do lote muda
nome de arquivo e contagem de arquivos (ADR 0010) — e é **exatamente o eixo em que a
tensão do `is_current` explodiria** se alguém tornasse o silver incremental. O teste é o
guarda que faz essa pessoa descobrir o problema no dia em que o introduzir, em vez de meses
depois.

O teste do BCB invoca o **backfill explícito**, não o agendador, porque tem que exercitar o
mesmo caminho que o README demonstra.

## Onde os metadados do Airflow moram

O Airflow precisa de banco de metadados. **SQLite foi eliminado por um fato, não por
preferência**, e o fato só apareceu porque foi verificado dentro da própria imagem:

```
from airflow.executors.sequential_executor import SequentialExecutor
  -> ModuleNotFoundError: No module named 'airflow.executors.sequential_executor'

executores em 3.3.1 -> ['CeleryExecutor', 'KubernetesExecutor', 'LocalExecutor']
core.executor (default) -> LocalExecutor
```

O `SequentialExecutor` **não existe mais na linha 3**. Os que restam rodam tarefas em
paralelo e exigem banco com escrita concorrente, o que o SQLite não oferece. A opção mais
simples deixou de estar disponível.

**Decisão: o mesmo container Postgres, num banco separado**, com papel próprio criado no
`db/init`. Não adiciona serviço ao `compose up`, e é o arranjo que qualquer deploy real
usa. SQLite aqui sugeriria que o arranjo de produção não foi considerado, e neste caso
ele nem é possível.

Rejeitado: **um segundo Postgres só para os metadados**. Separação mais limpa e um serviço
a mais para quem só quer ver o projeto rodar.

**Consequência aceita: `make reset` leva junto o histórico de execuções das DAGs**, porque
destrói o volume que agora hospeda os dois bancos. Isso é o que "reset" significa, e o
`airflow db migrate` roda na subida — idempotente — então o Airflow reconstrói o próprio
schema sozinho depois. A alternativa seria um reset seletivo que recria só o banco
`payments` e reaplica o `db/init` por fora, criando **dois caminhos de inicialização de
schema** — a mesma bifurcação rejeitada no ADR 0015.

## Consequências

- `catchup=False` nas duas é **declaração deliberada**, não esquecimento. Registrado aqui
  para que a próxima pessoa não "conserte" ligando.
- Airflow **3.3.1**, Python **3.12**. Versão corrente verificada no PyPI, que declara
  `requires_python >=3.10` e suporte até 3.14; 3.12 fica no meio da faixa, longe das bordas.
- O gold pode rodar sem a cotação do dia existir, e isso é **correto**, não falha. É o
  `chaos-fx-gap` acontecendo sozinho, com o detector já no lugar.
- Reprocessar a metade CDC significa reconstruir do bronze. O bronze é a fonte de verdade
  do reprocessamento, que é a razão de ele guardar o evento cru desde o ADR 0013.

## Emenda de 2026-08-27 — o consumidor vira serviço, e a DAG vira `bronze_to_gold`

A decisão original punha o consumidor CDC como primeira tarefa da DAG. **O raciocínio deste
ADR não muda** — o CDC continua sem data, o slot continua com posição e não com calendário,
e continuam sendo duas DAGs pelo mesmo motivo. O que muda é **onde o consumidor roda**.

### O achado, que vale mais que a justificativa arquitetural

A fatia vertical do heartbeat foi construída antes de qualquer alerta existir, e revelou
que **o alerta de heartbeat no desenho antigo teria sido inútil dos dois jeitos possíveis**.

Como tarefa de DAG, o consumidor vive ~25 segundos a cada 900:

```
fracao do tempo no ar : 2.8%
raspagens a cada 15s  : 1 em cada 36 encontra o processo vivo
```

E o contador reinicia do zero a cada execução, porque é processo novo. Sobre essa série,
o alerta teria dois destinos e nenhum deles serve:

- com **"No Data" tratado como Normal**, ele ficaria **cego** — sem série na maior parte do
  tempo, e sem série sendo OK, nunca dispararia;
- com **"No Data" tratado como Alerting**, ele **gritaria o tempo todo**, porque a ausência
  é o estado normal desse desenho.

Isto é o ADR 0006 acontecendo por acidente, dentro do próprio projeto, antes de chegarmos
ao cenário construído para demonstrá-lo. Um alerta pode estar cego por causa de uma
decisão de arquitetura tomada meses antes, sem ninguém escrever nada errado no alerta.
É evidência encontrada, não demonstração montada, e é o motivo de este parágrafo existir.

### A decisão

O consumidor passa a ser **serviço de longa duração** no Compose, com `CDC_IDLE_TIMEOUT=0`,
expondo `/metrics`. A DAG perde a tarefa de drenagem e se chama **`bronze_to_gold`**, porque
`cdc_to_gold` deixou de descrever o que ela faz: ela transforma o bronze que o serviço
alimenta.

`restart: "no"` no serviço, deliberadamente. O ADR 0018 diz que uma violação de contrato
para o consumidor e a recuperação é decisão humana; uma política de reinício transformaria
isso num laço de morte e renascimento, que é barulho e não sinal.

### Isto não fura a cerca da seção 2 do plano

O plano proíbe "streaming de verdade". Um consumidor de longa duração **não é
processamento de stream**: ele lê o WAL e escreve arquivo, e toda a transformação continua
em lote, de quinze em quinze minutos, disparada pelo Airflow. Não há janela deslizante, não
há estado de streaming, não há Kafka nem Flink. O que mudou foi **parar de fingir que a
ingestão tem agendamento** — ela nunca teve, e o ADR inteiro acima é sobre isso.

### Como os testes convivem com o serviço

Verificado antes de decidir: **cada teste já usa slot próprio**, e slots coexistem na mesma
publication — o teste de decomposição por lote prova isso com dois slots simultâneos. Não
existe disputa por slot.

A disputa real é pelas **tabelas**. Os fixtures apagam todas as linhas, o que poluiria o
bronze de desenvolvimento, e o teste do critério A2 altera o tipo de uma coluna, o que
**mataria o serviço para valer** — pelo ADR 0018, o backlog do slot continuaria carregando
a forma antiga mesmo depois de a origem ser revertida.

Por isso a suíte exige o serviço parado. `make test-e2e` e `make test-chaos` param e
religam sozinhos, e uma guarda de sessão no `conftest.py` falha na primeira linha com a
instrução, para que rodar `pytest` na mão dê uma frase em vez de um mistério.

Alternativa rejeitada: publicação e tabelas separadas para teste. Nenhuma separação de
publicação salva do `ALTER TABLE`, que é o caso fatal.

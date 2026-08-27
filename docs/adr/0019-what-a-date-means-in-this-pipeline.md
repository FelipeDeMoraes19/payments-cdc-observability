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
usa — SQLite num repositório de portfólio se lê como desconhecimento do arranjo de
produção, a menos que um documento diga o contrário, e aqui nem é possível.

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

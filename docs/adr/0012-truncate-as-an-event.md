# ADR 0012 — `TRUNCATE` é evento no bronze, e o que isso ainda não resolve

- Status: aceito
- Data: 2026-08-26

## Contexto

O decoder cobria `B R I U D C` e rejeitava qualquer outro tipo de mensagem com erro claro.
Isso significava que um `TRUNCATE` numa tabela publicada **derrubava o consumidor** — não
por decisão, mas por omissão. A omissão precisa virar decisão.

## Decisão

`TRUNCATE` é decodificado e gravado no bronze como evento, com `action: "truncate"`.

Uma mensagem `Truncate` pode citar várias relações, porque `TRUNCATE a, b, c` é um
comando só. Cada relação atingida vira **um registro no bronze**, todos com o mesmo LSN,
porque o bronze é particionado por tabela e um registro que fale de três tabelas não teria
onde morar.

O registro não tem tupla: `key`, `before` e `after` são nulos. Carrega
`truncate_options` com `cascade` e `restart_identity`, que é a informação que o `pgoutput`
de fato entrega.

## Alternativas rejeitadas

**Continuar falhando alto** — o comportamento anterior. Defensável no papel: `TRUNCATE` é
raro e destrutivo. Rejeitado pelo mesmo motivo que `status` não é enum no ADR 0011: é uma
operação **legítima** da origem, e transformar ação rotineira de quem administra o banco
em incidente de pipeline é o tipo de rigidez que faz gente desligar o alerta. O contrato
de bronze existe para pegar mudança de **forma**, não para vetar operação de DML.

**Ignorar em silêncio.** A pior das três. O bronze continuaria com linhas que não existem
mais na origem, e nada em lugar nenhum diria isso. É exatamente o modo de falha que este
repositório existe para não ter.

**Traduzir para N eventos de `delete`.** Seria o mais útil para o silver, e é impossível
de fazer honestamente: o `pgoutput` não manda as linhas apagadas — não mandar é o que
distingue `TRUNCATE` de `DELETE`. O consumidor teria que consultar a origem para
descobrir o que sumiu, o que quebra o modelo de só ler o stream e ainda corre contra o
próprio `TRUNCATE`, que já apagou tudo.

## Consequências

**Registrar o evento não é aplicar o evento.** Esta é a parte que precisa estar escrita.
O bronze agora sabe que a tabela foi truncada num LSN. Interpretar isso — "toda linha
desta tabela com LSN anterior a este deixou de existir" — é trabalho do silver, e o silver
é Marco 2. Até lá, um `TRUNCATE` na origem produz um bronze que sabe e um gold que não.
Isso é dívida consciente, não descuido, e some quando o job PySpark nascer.

**A chave de dedup do ADR 0002 não se aplica.** Sem tupla não há `pk`. A identidade de um
evento de truncate é `(tabela, lsn)`. Qualquer código que assuma `key` não nulo em todo
registro do bronze quebra aqui — e é bom que quebre alto.

## Evidência

Medido em 2026-08-26. `TRUNCATE payments, customers, merchants RESTART IDENTITY` produziu
três registros, um por tabela, todos no LSN `0/19F8560`, com
`{'cascade': False, 'restart_identity': True}`.

Uma suposição do decoder foi verificada em vez de presumida: `relation_for` exige que a
mensagem `Relation` já tenha chegado, e um `TRUNCATE` pode ser a **primeira** coisa que
uma tabela vê na sessão. Testado com slot novo e nenhum DML anterior: o `pgoutput` manda o
`Relation` antes do `Truncate`, e o consumidor decodifica sem erro. Se não mandasse, o
consumidor morreria justamente no caso raro — o pior lugar para descobrir.

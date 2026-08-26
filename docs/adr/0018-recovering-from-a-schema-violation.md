# ADR 0018 — Recuperação de uma violação de contrato, e por que reverter a origem não basta

- Status: aceito
- Data: 2026-08-26

## Contexto

Este ADR nasceu de um acidente, não de um planejamento. Depois de uma execução da suíte, o
consumidor CDC recusou-se a rodar:

```
contract violation: column public.payments.amount changed type at LSN 0/1AAB820:
the contract expects numeric (oid 1700), the stream carries text (oid 25)
```

E a coluna, no banco, **já estava de volta em `numeric`**. O teste do A2 altera o tipo,
verifica que o consumidor falha alto, e restaura a coluna na saída. A origem estava
correta. O consumidor continuava travado.

A causa é evidente depois de vista e não estava prevista em lugar nenhum: **o slot guarda
o backlog**. As mudanças gravadas enquanto a coluna era `text` continuam no WAL retido, e
o `pgoutput` as decodifica com a forma que elas tinham quando foram escritas. Consertar a
origem conserta o **futuro** do stream; não conserta o que já está na fila.

## Decisão

**O consumidor para, e a recuperação é decisão humana explícita.** Não há retomada
automática, não há pulo automático, e o processo continua saindo com código 2 até que
alguém decida o que fazer com o backlog.

Os dois caminhos de recuperação ficam documentados, com o custo de cada um:

**Descartar o backlog.** `pg_replication_slot_advance` até depois da região afetada. É
rápido e **perde as mudanças daquele intervalo** — elas nunca chegam ao bronze. Aceitável
quando o intervalo é conhecidamente lixo, como o de um teste.

**Ensinar o contrato a atravessar.** Declarar a forma antiga como aceita até um LSN de
corte, drenar, e então remover a tolerância. Preserva todo o dado e exige um contrato com
noção de validade por intervalo de LSN, que hoje não existe.

## Alternativas rejeitadas

**Pular automaticamente a mudança que viola o contrato e seguir.** É o comportamento que
mais parece "resiliente" e é o pior possível: o pipeline continuaria verde, o bronze
ficaria com um buraco do tamanho exato do problema, e ninguém saberia. Descartar dado é
uma decisão de negócio; código nenhum deveria tomá-la sozinho às três da manhã.

**Fazer o consumidor reler o schema atual da origem e usá-lo.** Parece resolver e inverte
o sentido do contrato: passaria a validar o stream contra o que a origem é **agora**, e
não contra o que o projeto declarou esperar. Uma origem que mudou de tipo duas vezes
passaria a validar contra a segunda mudança e aceitaria silenciosamente a primeira.
Contrato que se ajusta ao observado não é contrato.

**Recriar o slot.** Resolve na aparência e é a pior versão de descartar o backlog: perde
tudo o que não foi consumido, sem sequer delimitar o intervalo perdido.

## Consequências

- **Reverter o schema na origem não desbloqueia o consumidor.** Este é o parágrafo que
  economiza a próxima hora de confusão de quem operar isto, inclusive o autor, que já
  perdeu essa hora uma vez.
- Enquanto o consumidor está parado, **o slot não avança e o WAL se acumula**. Uma violação
  de contrato não tratada vira, com tempo suficiente, o modo de falha do slot órfão do
  ADR 0008. Os dois estão ligados, e o alerta de WAL retido do Marco 3 cobre os dois.
- O modo de falha entra na tabela do README com injeção óbvia: alterar o tipo, reverter, e
  observar que o consumidor continua parado.
- O contrato com validade por intervalo de LSN fica registrado como o caminho certo e não
  é construído agora. Ele só se paga quando existe um humano de plantão que não pode
  perder dado — situação que este projeto não tem.

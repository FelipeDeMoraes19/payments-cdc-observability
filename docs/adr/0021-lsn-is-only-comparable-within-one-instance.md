# ADR 0021 — Um LSN só é comparável dentro de uma instância do Postgres

- Status: aceito
- Data: 2026-08-27

## Contexto

Este ADR nasceu de um dado silenciosamente errado, e o defeito não estava em nenhum código
— estava num invariante que nunca tinha sido escrito.

Durante o Marco 3 rodei `docker compose down -v` duas vezes para recriar um papel do banco.
Isso destrói o volume e o Postgres nasce de novo, com o WAL **recomeçando perto do zero**.
O que não é destruído é `data/bronze`, que fica no disco do host.

O resultado: o bronze passou a conter duas sequências de LSN de dois bancos diferentes, com
os LSN antigos **maiores** que os da instância nova. O sintoma não foi erro. Foi isto:

```
confirmed_flush_lsn do banco  :  0/23C0CA8   (~37,5 milhoes)
maior lsn dentro do bronze    :  43.489.584
```

Um evento de `truncate` da instância morta carregava um LSN gigante, e a regra do ADR 0016
— toda chave cuja última mudança antecede o truncate vira `is_deleted` — marcou **todas as
linhas da instância nova como apagadas**. O silver ficou com 2613 linhas vigentes e zero
vivas, e nada falhou.

O ADR 0002 diz para deduplicar por `(pk, lsn)`. O ADR 0013 guarda `lsn_numeric` para
ordenar. Nenhum dos dois dizia a condição sob a qual essas comparações significam algo:

> **Um LSN é uma posição dentro do WAL de uma instância. Entre instâncias, dois LSN não são
> maior, menor nem iguais — eles não são comparáveis.**

## Decisão

Duas camadas, porque elas não são alternativas.

**Prevenção: `make reset` apaga `data/bronze`, `data/silver` e `data/gold`.** Ele destrói o
volume, então esses diretórios passam a descrever um banco que não existe mais. É o caso
comum, e o caso comum é o próprio autor.

**Detecção: o bronze grava `source_system_id`** — o `system_identifier` de
`pg_control_system()`, que é único por instância — e o silver **recusa processar** bronze
que contenha mais de um.

**Identificador ausente é violação, não desconhecido.** Bronze escrito antes desta coluna
não passa. Tratar ausência como "provavelmente tudo bem" reconstruiria exatamente o buraco
que a checagem existe para fechar, e o bronze aqui é sintético e se regenera, então exigir
custa zero.

## Alternativas rejeitadas

**Só a prevenção.** Um `rm -rf` numa linha de Makefile é um invariante que a primeira
pessoa apressada remove sem entender o que está removendo. E ela não cobre o volume apagado
à mão, o bronze copiado entre máquinas, nem o backup restaurado.

**Só a detecção.** Funciona e cobra caro: todo `make reset` passaria a exigir reingestão
completa porque o bronze antigo vira lixo detectado em vez de lixo prevenido.

**Reescrever LSN para uma sequência global** ao ingerir. Faria o dado atravessar instâncias
e destruiria a propriedade que torna o LSN útil — ele deixa de ser a posição que o Postgres
confirma no slot, e a chave do ADR 0002 vira um número inventado por nós.

**Aceitar e ordenar por `commit_time`.** Já rejeitado no ADR 0002 por motivos que continuam
valendo: relógio de quem escreve, colisão dentro do mesmo milissegundo, e nenhuma garantia
de monotonicidade.

## Consequências

- Trocar de instância passa a ser um evento com consequência declarada, em vez de uma
  operação que parece inofensiva.
- A mensagem do silver nomeia o problema e diz o comando: `run make reset and let it
  rebuild`. Erro que não diz o que fazer é meio erro.
- O bronze ganha uma coluna. Custo desprezível ao lado de dado errado que parece certo.

## Por que isto não entra na seção dos três bugs do README

Os três — consumidor como tarefa de lote, `retries` no lugar de `start_period`, gerador sob
demanda — foram a mesma suposição errada **sobre qual é o normal** contra o qual um detector
mede. Este é de outra família: **um invariante que nunca foi escrito em lugar nenhum**, e
que por isso ninguém violou de propósito nem verificou. Ele merece linha própria na tabela e
este documento, não um parágrafo dentro de uma seção sobre outra coisa.

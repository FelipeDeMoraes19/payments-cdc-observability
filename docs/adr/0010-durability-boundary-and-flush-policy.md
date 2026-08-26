# ADR 0010 — Fronteira de durabilidade e política de flush

- Status: aceito
- Data: 2026-08-26

## Contexto

O ADR 0008 decidiu que o checkpoint mora no slot e que o consumidor grava o bronze antes
de confirmar. Falta dizer **quando** ele grava, e **onde exatamente** fica a fronteira
entre "durável" e "confirmado".

## Decisão

**A fronteira é o `os.replace`.** O consumidor acumula registros em memória, grava um
arquivo temporário, chama `fsync`, renomeia atomicamente, e **só então** chama
`send_feedback`. Nada é confirmado antes de estar no disco com nome definitivo.

**O lote fecha por contagem ou por tempo, o que vier primeiro** — 500 registros ou 5
segundos — **mas só corta em fronteira de `Commit`.** Uma transação nunca é partida entre
dois arquivos.

**A partição é por data de commit**, não por data de ingestão:
`data/bronze/cdc/<tabela>/dt=<data do commit>/part-<LSN inicial>-<LSN final>.jsonl`.

O consumidor apaga arquivos `.tmp` órfãos ao subir. Eles são o rastro de uma morte no meio
de uma gravação, não têm nome definitivo e nunca foram confirmados.

## Alternativas rejeitadas

**Confirmar antes de gravar.** Elimina duplicata e cria perda silenciosa, que é
estritamente pior — duplicata se resolve com dedup, linha perdida não se resolve com nada.

**Gravar registro a registro, sem lote.** Simples e honesto, mas gera um arquivo por
mudança e um `send_feedback` por mudança. Com o volume deste projeto funcionaria; o
problema é que esconde a decisão. Micro-batch é o que uma plataforma real faz, e o lote é
o que dá sentido ao intervalo de LSN no nome do arquivo.

**Cortar o lote por tamanho de arquivo ou a qualquer momento, inclusive no meio de uma
transação.** Com dedup exato por `(pk, lsn)` não haveria perda nem inconsistência. Foi
rejeitado por legibilidade: um arquivo com metade de uma transação é impossível de
explicar para quem lê o bronze, e o intervalo de LSN no nome deixa de delimitar algo
íntegro.

**Particionar por data de ingestão.** Torna o nome do arquivo trivialmente determinístico,
e mata o modo de falha de dado atrasado: se tudo cai na partição de hoje, o teste de
freshness e o `chaos-late` não têm o que detectar. Tempo de evento é como o silver e o
gold raciocinam, então o bronze particiona por tempo de evento.

## Consequências

- **A janela de duplicata é de microssegundos.** Ela existe só entre o `os.replace` e o
  pacote de feedback chegar ao servidor. Isso é bom em produção e ruim para testar: um
  `kill -9` em momento aleatório praticamente nunca cai nela. Medido: um teste de restart
  com 414 registros e uma morte no meio da carga produziu **zero** duplicatas. A cláusula
  "toda duplicata é exata" do critério A1 é, portanto, **impossível de exercitar por
  acaso**, e exige injeção deliberada — ver abaixo.
- **O nome do arquivo não basta como idempotência.** Um replay com as mesmas fronteiras de
  lote regrava o mesmo nome e sobrescreve. Um replay com fronteiras diferentes — que é o
  caso normal, porque elas dependem de tempo — gera arquivos de nomes diferentes com
  conteúdo sobreposto. Quem garante correção é o dedup por `(pk, lsn)`, não o nome. O nome
  só evita o caso fácil.
- Duplicata exata é exata **no evento**, não no registro inteiro: `ingested_at` é gravado
  na decodificação e difere entre a cópia original e a reentregue. Qualquer comparação de
  duplicata tem que excluir metadado de ingestão.
- Lote curto gera muitos arquivos pequenos. Com 1 segundo de lote, 414 registros viraram
  67 arquivos. O volume aqui é pequeno de propósito e o plano proíbe otimização de
  performance (seção 2), então fica registrado e não tratado.
- `fsync` no arquivo não é `fsync` no diretório. Em POSIX, a renomeação só é garantida
  após sincronizar o diretório também; no Windows isso não existe como operação. A
  garantia real aqui é "o arquivo tem conteúdo completo ou não tem nome definitivo", que é
  suficiente para o critério de aceite e não é a mesma coisa que durabilidade contra queda
  de energia.

## Ponto de injeção de falha

`CDC_FAIL_BEFORE_FEEDBACK=1` mata o processo com `os._exit(17)` depois da gravação e antes
da confirmação. É a única forma honesta de abrir a janela acima: o slot não rebobina
(ADR 0008) e dois consumidores não compartilham um slot, então não existe outro caminho.

Ele foi antecipado do Marco 3 para cá porque sem ele o critério A1 fecharia pela metade.
O `make chaos-replay` do Marco 3 passa a ser um alias para este gancho, não código novo.

Alternativa rejeitada: manter o gancho fora do código de produção, num subclasse ou num
patch aplicado só em teste. Esconde o modo de falha justamente no lugar onde o projeto
quer exibi-lo, e um caminho de falha que só existe em teste não é o caminho que roda.

## Evidência

`send_feedback` do psycopg2 tem `force=False` por padrão, e nesse modo **não transmite
nada**: ele só atualiza o contador interno e espera o `status_interval`, que é de 10
segundos. As primeiras execuções duravam menos que isso, chamavam `send_feedback` cinco
vezes e o `confirmed_flush_lsn` não saía de `0/1984788` — o consumidor parecia funcionar e
reprocessava tudo a cada reinício.

Com `force=True`:

```
antes  : 0/1984788
depois : 0/198BBB0
segunda execucao: records written: 0
WAL retido: 14 kB -> 56 bytes
```

Confirmação imediata não é detalhe de configuração: sem ela, a fronteira de durabilidade
decidida acima não existe na prática.

O critério A1 está coberto por dois testes, e cada um prova uma metade:

| Teste | Injeção | Medido |
|---|---|---|
| `kill -9` no meio da carga | morte em momento aleatório | 414 registros, 0 perdidos, 0 reentregues |
| gravar e morrer antes de confirmar | `CDC_FAIL_BEFORE_FEEDBACK=1` | 50 registros, 26 identidades, **24 reentregues, 0 eventos em conflito** |

O primeiro prova que nada se perde. O segundo prova que o que volta, volta idêntico — e
só produz duplicata porque a falha é injetada exatamente na janela.

# ADR 0009 — Papel de replicação dedicado, e onde mora a senha dele

- Status: aceito
- Data: 2026-08-25

## Contexto

O consumidor CDC precisa de uma conexão de replicação. No spike ele usou o superusuário
do Postgres, o que funciona e não se sustenta: o consumidor só lê, e um repositório
público não deve ensinar a rodar CDC como superusuário.

Junto vem uma segunda pergunta, que é a chata: se existe um papel com senha, **onde a
senha mora** num repositório que é público desde o primeiro commit.

## Decisão

**Papel dedicado `cdc`**, com `LOGIN` e `REPLICATION`, sem `SUPERUSER`. Recebe `CONNECT`
no banco, `USAGE` no schema e `SELECT` nas tabelas publicadas. Criado por
`db/init/003_cdc_role.sh`, que falha alto se as variáveis não existirem.

**A senha mora no `.env`, que está no `.gitignore`.** O `.env.example` vai commitado com
os mesmos valores.

O segundo ponto merece ser dito sem rodeio: **não existe segredo aqui para proteger.** O
Postgres é um container descartável, publicado em `localhost:5434`, populado por gerador
sintético. A senha do `.env.example` é `cdc-local-only` e é literalmente o valor usado. O
`.env` existe pela disciplina, não pelo sigilo — a hora de descobrir que a credencial
estava no código é antes de o projeto ter alguma credencial que importe.

## Alternativas rejeitadas

**Continuar no superusuário.** Zero configuração e ensina o hábito errado. Um avaliador
que abra o `compose.yaml` e veja o consumidor entrando como superusuário aprende sobre
mim exatamente o que eu não quero ensinar.

**Senha fixa no `compose.yaml`.** É o que quase todo projeto de portfólio faz. Funciona,
e transforma o repositório em exemplo de credencial versionada. O custo de fazer certo
aqui é um arquivo e três linhas.

**Cofre de segredo de verdade** — Vault, SOPS, `docker secret`. Rejeitado por
desproporção. Protege um valor que não é secreto, custa uma dependência nova e uma etapa
de setup para quem só quer rodar `docker compose up`. O plano proíbe autenticação e
infraestrutura acessória (seção 2) justamente para o projeto não virar isso.

**Ler a senha de variável de ambiente exportada na sessão, sem `.env`.** Mais puro, e
hostil no Windows: exige reexportar a cada terminal novo e quebra o `docker compose up`
sem aviso claro. O `.env` é lido pelo Compose e pelo consumidor com o mesmo arquivo.

## Consequências

- `docker compose up` **falha imediatamente** sem `.env`, com mensagem dizendo o que
  copiar, porque as variáveis do Compose usam a forma `${CDC_USER:?...}`. Falhar na
  subida é melhor que subir um Postgres sem o papel e descobrir depois.
- Conexão lógica de replicação passa pelas regras normais do `pg_hba`, e não pela linha
  `replication`, porque conexão de replicação lógica especifica um banco. O papel novo
  conecta sem ajuste no `pg_hba.conf`.
- Se um dia o projeto ganhar credencial que importe — não está previsto — o caminho já
  está montado: troca o conteúdo do `.env`, e nada no histórico precisa ser reescrito.
- `db/init/003_cdc_role.sh` é shell, e shell com CRLF não roda em container Linux. O
  `.gitattributes` fixa `*.sh` em LF. Sem ele, o arquivo quebraria só na máquina de quem
  clonasse no Windows, que é o pior tipo de bug.

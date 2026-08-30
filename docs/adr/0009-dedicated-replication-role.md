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

## Emenda de 2026-08-29 — a senha do Grafana também não é segredo, e tratá-la como um quebrou o clone

A senha de admin do Grafana nasceu como segredo gerado, ao lado da chave de PII. Isso
estava errado, e o erro só apareceu ao clonar o repositório público e rodá-lo — o único
teste que ninguém tinha feito.

**O Grafana lê `GF_SECURITY_ADMIN_PASSWORD` apenas quando cria o próprio banco.** Depois
disso o volume manda, e toda alteração no `.env` é ignorada **em silêncio**. O sintoma é um
`401` do Terraform, num comando que não fala de senha em lugar nenhum. Medido: nem a senha
antiga nem a nova autenticavam, porque o volume carregava uma terceira, de um ciclo
anterior.

Medido em 2026-08-29, alterando só o `.env` e recriando o container:

```
senha nova do .env, apos recriar o container : 401
senha que o volume guarda                    : 200
```

> **Correção de 2026-08-30.** Este ADR afirmava que `grafana cli admin reset-admin-password`
> responde `Admin password changed successfully ✔` **sem ter efeito**. **Isso é falso** e foi
> retirado. Reteste em 2026-08-30, nas duas formas de invocação: o comando **funciona** — a
> senha nova passa a autenticar com `200` e a anterior passa a `401`.
>
> O que aconteceu no dia foi um `401` observado logo depois de rodar o comando, do qual eu
> concluí um mecanismo causal sem isolar a variável. Não reproduz. A causa real daquele `401`
> não foi diagnosticada, e registrar "não sei" é mais honesto que manter uma explicação
> convincente e errada num documento permanente.
>
> O erro não é o diagnóstico ruim: é ter escrito um mecanismo afirmativo em registro
> permanente a partir de **uma observação não isolada**, e ele ficou de pé por um dia — até
> alguém tentar construir em cima. É exatamente a falha que este repositório documenta,
> cometida por quem o escreve.

**Decisão: a senha do Grafana passa a ser valor fixo e declarado**, `grafana-local-only`,
pelo mesmo argumento que o ADR já faz para o Postgres local. É um painel numa porta de
`localhost`, com dado sintético, e gerar segredo para ele criou um problema de rotação em
troca de nenhuma segurança. **A chave de PII continua gerada** — aquela é segredo de
verdade, e a diferença entre as duas é justamente o ponto deste ADR.

E o `make alerts` passa a checar a autenticação antes do `apply`, para trocar um `401` sem
contexto por uma frase que diz o que aconteceu e qual comando resolve.

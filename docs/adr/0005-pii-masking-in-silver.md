# ADR 0005 — Mascaramento de PII no silver, com HMAC e chave gerenciada

- Status: aceito
- Data: 2026-08-26

## Contexto

`customers` carrega CPF e e-mail. O gold é o que alguém consulta, o `dbt docs` publica e um
avaliador abre. Dado pessoal não pode chegar lá em claro, e o projeto ainda precisa
conseguir **juntar** um cliente consigo mesmo ao longo do tempo, senão a `dim_customer` em
SCD2 perde o sentido.

## Decisão

**O mascaramento acontece no silver**, no job PySpark que lê o bronze. O bronze guarda o
evento cru, incluindo o CPF — é a fronteira de ingestão e mentir ali destruiria a
capacidade de reprocessar. Do silver em diante, o valor em claro não existe mais.

**A função é HMAC-SHA256 com chave**, não hash com sal:

```
cpf_masked = hex(HMAC-SHA256(key = PII_HMAC_KEY, message = cpf))
```

**A saída é digest hexadecimal puro, sem preservar formato.** Um CPF mascarado não se
parece com um CPF.

A chave vive em `PII_HMAC_KEY`, no `.env`, fora do git, e é a única coisa neste
repositório que é segredo de verdade — ao contrário da senha do Postgres local, que o
ADR 0009 declara explicitamente não ser.

## Alternativas rejeitadas

**Hash com sal (SHA-256 de `sal + cpf`).** Foi a primeira proposta e está errada. **O CPF
tem espaço de busca pequeno**: onze dígitos com dois de verificação dão cerca de 10⁹
valores válidos, o que uma máquina comum percorre em minutos. Com o sal conhecido, o hash
é reversível por força bruta — e um sal guardado no `.env`, ao lado da aplicação que o usa,
é recuperável por quem já tem o dado. Sal protege contra tabela arco-íris pré-computada;
não protege contra um adversário que pode gerar todos os CPFs. O HMAC muda o jogo porque a
chave não é derivável do dado nem do código.

**Preservar o formato do CPF na saída** (mascarado que parece CPF). Rejeitado por um motivo
que só aparece no Marco 3: o `chaos-pii` desliga o mascaramento e um teste procura padrão
de CPF no gold. Se o valor mascarado também parecer CPF, **o detector não distingue
mascarado de vazado** e o teste não prova nada. Com digest hexadecimal, o teste acha zero
com o mascaramento ligado e acha CPF de verdade quando o caos o desliga. O formato da saída
é o que torna a falha detectável.

**Tokenização com tabela de correspondência.** Reversível por desenho, o que é útil em
produção e transforma a tabela num alvo. Fora do escopo (plano, seção 2).

**Mascarar no gold, com o silver em claro.** Adia o problema e multiplica os lugares onde
o dado em claro existe. Quanto mais cedo o valor some, menos superfície existe.

**Remover a coluna.** Resolve a privacidade e mata o SCD2: sem identificador estável não há
como saber que duas versões são do mesmo cliente.

## Consequências

**Isto é pseudonimização, não anonimização.** Sob a LGPD, dado pseudonimizado **continua
sendo dado pessoal** — o artigo 12 só trata como anonimizado aquilo que não permite
reidentificação por meios razoáveis, e um valor que se reverte com a chave não se
qualifica. O controle efetivo aqui é **a gestão da chave**, não o algoritmo. Escrever
"hasheamos o CPF" num relatório de conformidade e dar o assunto por encerrado é o erro que
este ADR existe para não cometer.

**Rotacionar a chave quebra join histórico.** O mesmo CPF sob uma chave nova gera um digest
diferente, e toda a `dim_customer` deixa de reconhecer o cliente que já estava lá. Rotação
não é operação de rotina: é decisão de arquitetura, e exige ou reprocessar o silver inteiro
a partir do bronze — que ainda tem o valor cru, e por isso o bronze não mascara — ou
manter um mapa de digest antigo para novo, que é justamente a tabela de correspondência
rejeitada acima. Registrar isso agora é mais barato que descobrir na primeira rotação.

**O bronze continua com PII em claro.** É deliberado e é a razão de o reprocessamento
funcionar. Num sistema real isso moveria o problema para o controle de acesso ao bronze e
para a política de retenção, e nenhum dos dois está no escopo deste projeto. Fica dito.

**Sem `PII_HMAC_KEY` o job falha alto**, como o Compose falha sem `CDC_PASSWORD`. Um
mascaramento que silenciosamente vira no-op é pior que nenhum.

Todo o dado aqui é sintético. Nenhum CPF real passa por este código.

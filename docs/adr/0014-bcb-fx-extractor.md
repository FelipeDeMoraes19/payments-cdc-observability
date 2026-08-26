# ADR 0014 — Extrator do BCB: série SGS, janela mensal e watermark no próprio dado

- Status: aceito
- Data: 2026-08-26

## Contexto

A segunda fonte do pipeline é a cotação de câmbio do Banco Central. Ela existe por um
motivo concreto: o `amount_brl` do `fct_payment` sai de cruzar o pagamento com a cotação
do dia, e é o que conecta as duas metades do projeto. É também o padrão de ingestão
oposto ao do CDC — batch incremental com watermark, em vez de stream.

## Decisão

**API SGS**, uma série por moeda, em JSON, sem autenticação:
`USD` na série **1** e `EUR` na série **21619**, ambas PTAX de venda.

**A janela é mensal e o mês é reescrito inteiro.** Cada execução regrava
`data/bronze/fx/<MOEDA>/month=<AAAA-MM>/observations.parquet` com o mês completo. Nome
determinístico, sem sufixo de execução: rodar de novo sobrescreve em vez de acumular.

**O watermark é a maior `quote_date` presente no bronze**, e a retomada começa no primeiro
dia daquele mês — nunca num arquivo de estado à parte (ADR 0008). Um mês parcial é sempre
refeito por completo, o que torna a retomada e o backfill a mesma operação.

**A janela é validada antes da chamada.** Fim futuro é recusado com código 2.

## Alternativas rejeitadas

**API Olinda / PTAX (`CotacaoMoedaDia`)** em vez do SGS. Entrega compra e venda no mesmo
documento e é mais rica. Rejeitada para a ingestão por ser mais verbosa e por exigir uma
chamada por dia; o SGS entrega um intervalo inteiro numa requisição. Ela **foi** usada, e
de propósito, para uma coisa só: descobrir qual série SGS é a de venda — ver evidência.

**Arquivo por dia.** Simétrico ao "uma cotação por dia" e produz centenas de arquivos de
poucos bytes, com um dia de dado e um watermark que depende de listar o diretório inteiro.

**Nome de arquivo com o intervalo pedido**, como o bronze do CDC faz com o LSN. Funciona
para o CDC porque o intervalo de LSN é o dado. Aqui o intervalo é o **pedido**, não o
dado: duas janelas diferentes cobrindo o mesmo dia gerariam dois arquivos com a mesma
cotação e o rerun deixaria de ser idempotente. O mês é uma fronteira do calendário, não do
pedido, e por isso serve de identidade.

**Watermark em arquivo de controle.** Mesma rejeição do ADR 0008: duas fontes de verdade
que divergem exatamente quando o processo morre no meio.

## Consequências

- **404 é ambíguo na origem.** Medido: a BCB devolve `404` para fim de semana sem cotação,
  para data futura e para intervalo invertido — o mesmo código para "vazio" e para "pedido
  errado". Tratar 404 como "sem cotação" é obrigatório, porque fim de semana é o caso
  comum. Para que isso não engula um erro real, a janela é validada localmente antes da
  chamada; com isso, o único 404 que sobra é o legítimo.
- **Dia não útil simplesmente não existe** no bronze. Não é falha, é o calendário — e é a
  lacuna que o `chaos-fx-gap` e o teste `not_null` em `amount_brl` vão explorar no Marco 3.
- **Idempotência é da observação, não do byte.** Rerun sobre a mesma janela produz o mesmo
  conjunto de arquivos e as mesmas observações, com bytes diferentes, porque `ingested_at`
  é carimbado por execução. É a mesma distinção que o ADR 0010 faz para duplicata de
  replay.
- Aqui o bronze é **tipado** — `quote_date` como `date32` e `rate_brl` como `decimal128` —
  enquanto o bronze do CDC guarda tudo como texto (ADR 0013). A diferença é deliberada e
  não é incoerência: no CDC o schema da origem pode derivar e o bronze precisa registrar a
  deriva em vez de falhar sobre ela. Aqui o contrato já validou na fronteira, e a forma
  crua carrega uma **ambiguidade de localidade** — `01/06/2026` é 1 de junho no Brasil e 6
  de janeiro nos Estados Unidos. Guardar isso como texto não é guardar o dado cru, é
  empurrar uma bomba-relógio para a camada seguinte.
- `requests` entra no `requirements.txt`.

## Evidência

**Qual série é a de venda, sem adivinhação.** As séries 21619 e 21620 diferem em 0,0012 e
as duas parecem EUR/BRL. Em vez de inferir pela ordem de grandeza, a API do PTAX foi
consultada para 05/08/2026:

```
PTAX  USD  compra=5.1148  venda=5.1154
PTAX  EUR  compra=5.9050  venda=5.9062
SGS   1      -> 5.1154      (USD venda)
SGS   21619  -> 5.9062000   (EUR venda)
SGS   21620  -> 5.9050000   (EUR compra)
```

**A chave de partição não pode sombrear uma coluna do arquivo.** O layout original era
`currency=USD/month=.../`, com `currency` também gravado dentro do Parquet. Ler o
diretório como dataset falhava:

```
ArrowTypeError: Unable to merge: Field currency has incompatible types:
string vs dictionary<values=string, indices=int32, ordered=0>
```

O bronze do CDC não sofria disso porque lá a tabela é um segmento simples de caminho e só
`dt=` é chave Hive. O layout do FX foi alinhado ao mesmo padrão, e o diretório inteiro
passou a ler como um dataset de 124 linhas.

**O contrato exigia menos do que devia.** A primeira versão do validador devolvia o valor
intacto quando a data não casava com `DD/MM/YYYY`, e o Pydantic então aceitava `2026-06-01`
de bom grado. Uma troca de formato na origem passaria em silêncio — o oposto do que um
contrato de fronteira serve para fazer. Agora o formato brasileiro é obrigatório e
qualquer outro levanta violação. Encontrado pelo teste, não por leitura.

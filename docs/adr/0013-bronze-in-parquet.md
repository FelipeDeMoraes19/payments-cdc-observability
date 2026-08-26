# ADR 0013 — Bronze em Parquet, com a tupla como `MAP` e o LSN em duas formas

- Status: aceito
- Data: 2026-08-26

## Contexto

O bronze nasceu em JSONL de propósito: a seção 13 do plano manda fazer a replicação
funcionar ponta a ponta antes de pensar em formato. Ela funciona, com os dois critérios do
Marco 1 provados. Agora o formato muda, e com ele três decisões que o JSONL não obrigava a
tomar: quem escreve, que forma a tupla assume, e como o LSN é guardado.

## Decisão

**`pyarrow` escreve o Parquet**, direto do consumidor, uma linha por mudança. Compressão
**zstd**. O caminho e o nome do arquivo não mudam:
`data/bronze/cdc/<tabela>/dt=<data do commit>/part-<LSN inicial>-<LSN final>.parquet`,
com o mesmo `.tmp` + `fsync` + `os.replace` do ADR 0010.

**A tupla é `MAP<string, string>`.** `key`, `before` e `after` guardam o valor exatamente
como o `pgoutput` entregou: texto. Um schema serve para as três tabelas.

**O LSN é gravado duas vezes:** `lsn` como texto (`0/1B08670`, o que o Postgres mostra) e
`lsn_numeric` como `int64`. Igualdade pode usar qualquer um dos dois; **ordenação tem que
usar `lsn_numeric`**.

`schema` e `table` viraram `schema_name` e `table_name`, porque as duas primeiras exigem
aspas em quase todo dialeto SQL e o bronze existe para ser consultado.

## Alternativas rejeitadas

**Manter JSONL.** Legível e honesto, e errado para a camada que todo o resto lê: cada
leitura reparsa texto e nenhum motor consegue empurrar predicado para dentro do arquivo. O
plano pede Parquet na seção 13 justamente aqui.

**Esperar o PySpark do Marco 2 escrever o bronze.** Inverte a ordem do plano e cria uma
dependência absurda: o consumidor de CDC precisaria de uma JVM para conseguir **persistir**
qualquer coisa. O consumidor tem que se sustentar sozinho.

**Tupla como `struct` tipado, uma coluna Parquet por coluna da tabela.** É o mais
confortável para quem consome, e contradiz o que o bronze é. Tipagem é trabalho do silver
(plano, seção 4). Um bronze tipado obrigaria o escritor a conhecer o tipo de cada coluna —
duplicando o contrato — e o faria **falhar** diante de um drift que ele deveria apenas
registrar. Bronze guarda o que chegou; quem decide se aquilo é um `numeric` é a camada
seguinte.

**Tupla como string JSON dentro do Parquet.** Simples e perde o principal: sem estrutura,
`after['status']` deixa de existir e o bronze volta a exigir parsing para qualquer
pergunta.

**`fastparquet`.** Mais leve, menos padrão, e acoplado ao pandas. O `pyarrow` é o que o
DuckDB e o Spark do Marco 2 falam nativamente.

## Consequências

- **O LSN em texto ordena errado, e essa é a razão de `lsn_numeric` existir.** Medido:

```
sorted(['0/9FFFFFF', '0/10000000'])            -> ['0/10000000', '0/9FFFFFF']
ordenado por valor numerico                    -> ['0/9FFFFFF', '0/10000000']
```

  `0x9FFFFFF` é 167.772.159 e `0x10000000` é 268.435.456. O texto inverte os dois. O
  "vence a versão de maior LSN" do silver escolheria a versão **errada** — em silêncio, e
  só depois de uns 256 MB de WAL, ou seja, tarde e longe da causa. Sem esta coluna, o
  ADR 0002 estaria correto no papel e quebrado no dado.

- LSN é `uint64` no Postgres e `int64` no Parquet. O sinal só viraria problema depois de 8
  exabytes de WAL num único servidor. Registrado, não tratado.

- Todo valor da tupla é string, inclusive número e data. É intencional e é o que mantém o
  bronze cru.

- O `pyarrow` devolve `MAP` como lista de pares na leitura, não como dicionário. O leitor
  do bronze converte, para que quem consome em Python veja um dicionário.

- Arquivo pequeno continua pequeno: 291 registros viraram 2 arquivos e 21.765 bytes. O
  volume é pequeno de propósito e o plano proíbe otimização de performance (seção 2).

- `pydantic` e `pyarrow` entraram no `requirements.txt`. O `pydantic` já era usado pelos
  contratos e **não estava declarado** — num clone limpo, o consumidor quebraria na
  importação.

## Evidência

O bronze responde SQL sem carregar nada em lugar nenhum:

```sql
SELECT table_name, action, count(*) AS events
FROM read_parquet('data/bronze/cdc/**/*.parquet')
GROUP BY table_name, action ORDER BY table_name, action;
```

```
customers | update | 27
payments  | insert | 191
payments  | update | 73
```

E o dedup do ADR 0002 cabe numa consulta, usando `lsn_numeric` para ordenar:

```sql
SELECT key['payment_id'] AS payment_id, after['status'] AS status, after['amount'] AS amount, lsn
FROM read_parquet('data/bronze/cdc/payments/**/*.parquet')
WHERE action <> 'truncate'
QUALIFY row_number() OVER (PARTITION BY key['payment_id'] ORDER BY lsn_numeric DESC) = 1
ORDER BY payment_id::BIGINT LIMIT 5;
```

```
198 | failed     | 3709.04 | 0/1B08670
199 | pending    | 473.01  | 0/1B07FD8
200 | refunded   | 1106.10 | 0/1B08E58
201 | refunded   | 781.62  | 0/1B0C9C0
202 | authorized | 1326.28 | 0/1B0D458
```

Nota de ambiente: o `duckdb` desenha resultado com caracteres de caixa Unicode, e imprimir
isso num console `cp1252` levanta `UnicodeEncodeError`. Não é defeito do dado — é a
armadilha de console do Windows atingindo uma ferramenta em vez do código do projeto.

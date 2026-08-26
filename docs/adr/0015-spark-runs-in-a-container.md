# ADR 0015 — Spark roda em container, e por que Spark está aqui

- Status: aceito
- Data: 2026-08-26

## Contexto

O job bronze → silver é PySpark. Antes de escrever uma linha dele, duas perguntas
precisam de resposta honesta: **onde ele executa** e **por que é Spark**.

## Decisão

**Spark 4.2.0 com Java 21, dentro de um container**, em `local[*]`. Imagem oficial
`apache/spark:4.2.0-java21-python3` com uma camada fina por cima instalando o
`requirements.txt` do projeto, para que o job importe `contracts/` e escreva Parquet com o
mesmo `pyarrow` do resto.

O job é batch, não serviço: sobe com `docker compose run --rm`, e o serviço fica atrás de
um profile para não subir junto no `docker compose up`.

**PySpark não é instalado na máquina do autor.**

## Alternativas rejeitadas

**PySpark local no Windows.** Medido em 2026-08-26 nesta máquina:

```
pyspark      : ausente
java         : 1.8.0_172
HADOOP_HOME  : nao definido
winutils.exe : nao encontrado
```

Três problemas somados, e qualquer um deles bastaria.

O Java local é o **8**. O Spark 4 exige 17 ou mais, então rodar local prenderia o projeto
na linha 3.5, que já é antiga — e prenderia por causa de uma JVM instalada na máquina, o
que é uma razão ruim para escolher versão de framework.

O Spark no Windows exige `winutils.exe` e `HADOOP_HOME`, e nenhum dos dois existe aqui. A
forma usual de obtê-los é **baixar um binário Windows não assinado de um repositório
pessoal no GitHub**. Num repositório público de portfólio isso vira uma dependência
impossível de defender: não se sabe quem compilou, e ela entra no caminho de execução.

E o terceiro: quem clonar o repositório teria que repetir esses dois passos na própria
máquina, o que contradiz a tese de que tudo cabe num `docker compose up`.

O plano, seção 12, já previa esta saída antes de qualquer medição. A medição só confirmou.

**Upgrade do Java local para 21.** Resolve a versão e não resolve o `winutils`, nem o
problema de quem clona.

**Cluster Spark de verdade**, com master e workers no Compose. Pareceria mais impressionante
e seria encenação: `local[*]` num volume deste tamanho executa o mesmo plano lógico, e um
cluster de brinquedo com dois workers num laptop não demonstra nada sobre distribuição que
o `local[*]` já não demonstre. Fora do escopo (plano, seção 2).

**Trocar Spark por DuckDB ou pandas.** Tecnicamente a decisão certa para este volume — ver
abaixo — e derrota o propósito. Fica registrado como a alternativa que perde por um motivo
que não é técnico.

## A parte honesta: Spark é desnecessário neste volume

Este projeto processa alguns milhares de linhas. **Spark é a ferramenta errada para isso**,
e vale dizer com todas as letras em vez de encenar escala inexistente:

- O overhead de subir uma JVM e um plano distribuído é maior que o trabalho a ser feito.
- DuckDB responde as mesmas perguntas em milissegundos, num processo só — e de fato
  responde, no README, sobre o mesmo bronze.
- Nada aqui chega perto de precisar de shuffle particionado por rede.

O Spark está no projeto porque **a falta dele barrou o autor em processos seletivos
concretos**. É decisão de carreira, tomada de olhos abertos, e o projeto assume o custo.

O que dá para fazer, e é o que este repositório faz, é usá-lo de um jeito que **escalaria**:
uma passada de janela em vez de coleta para o driver, sem `collect()` no caminho quente,
com a deduplicação expressa como `row_number()` particionado pela chave. O código não muda
se o volume crescer mil vezes; só o `local[*]` viraria outra coisa.

Um projeto que admite que a ferramenta é maior que o problema, e explica por que ela está
ali, é mais defensável numa entrevista que um que finge o contrário e é desmontado na
primeira pergunta sobre volume.

## Consequências

- Iterar fica mais lento: cada execução passa por `docker compose run`. Mitigado por um
  alvo de shell interativo, para experimentar dentro do container.
- O código do job precisa ser importável de dentro do container, então o repositório é
  montado como volume e `contracts/` é importado de lá.
- A imagem carrega Java 21 e pesa centenas de megabytes. É custo de disco, pago uma vez.
- Quem clona não instala Java, Spark nem `winutils`. Só Docker.

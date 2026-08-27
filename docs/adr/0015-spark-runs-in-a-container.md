# ADR 0015 — Spark roda em container, e por que Spark está aqui

- Status: aceito
- Data: 2026-08-26

## Contexto

O job bronze → silver é PySpark. Antes de escrever uma linha dele, duas perguntas
precisam de resposta honesta: **onde ele executa** e **por que é Spark**.

## Decisão

**Spark 4.2.0 dentro de um container**, em `local[*]`. **PySpark não é instalado na máquina
do autor.**

> **Emenda de 2026-08-26.** A decisão original usava a imagem oficial
> `apache/spark:4.2.0-java21-python3` com uma camada fina por cima. **O raciocínio abaixo
> continua inteiro e a conclusão não mudou** — o Spark não roda no host, pelos três motivos
> medidos. O que mudou foi **qual imagem**, e por uma razão que só apareceu com o Airflow.
>
> Agora existe **uma imagem só**, construída sobre a imagem oficial do Airflow, com JRE 17,
> `pyspark` instalado por `pip` e o `dbt-duckdb` junto. O `make silver` e as tarefas da DAG
> usam **a mesma imagem**.
>
> O motivo decisivo não é robustez, é **bifurcação**: com o Airflow invocando o Spark, a
> alternativa seria ele chamar um container irmão pelo socket do Docker, e passariam a
> existir dois caminhos para rodar o mesmo job — o `make silver` por uma imagem e a DAG por
> outra. Dois caminhos divergem, e no dia em que divergirem o sintoma é *"funciona no make
> e falha na DAG"*, que é o pior tipo de defeito para diagnosticar. Caminho único ou nada.
>
> **Java 17, e o 21 foi considerado e descartado.** O Spark 4.2 suporta 17 oficialmente. O
> 17 vem do repositório principal do Debian bookworm, que é a base da imagem do Airflow; o
> 21 exigiria backports ou uma fonte de pacote externa. Adicionar fonte externa por uma
> diferença invisível neste volume é dívida que cobra depois, na hora em que o build quebra
> por um motivo que ninguém lembra.
>
> Rejeitado junto: **Airflow chamando containers irmãos pelo socket do Docker**. Mantém as
> imagens finas e o container do Airflow passa a precisar do CLI do Docker, do socket
> montado e do nome do projeto compose do hospedeiro. É docker-dentro-de-docker: um modo de
> falha novo que não ensina nada sobre dados.

O job é batch, não serviço: sobe com `docker compose run --rm`, e o serviço fica atrás de
um profile para não subir junto no `docker compose up`.

### Duas armadilhas da imagem do Airflow, medidas na consolidação

**O entrypoint trata o primeiro argumento como subcomando do `airflow`.** Rodar
`docker compose run runner spark-submit ...` produz
`airflow command error: argument GROUP_OR_COMMAND: invalid choice: 'spark-submit'`. O
entrypoint tem caso especial para `bash`, então toda tarefa é invocada como
`bash -c "..."`. Vale para o `make silver`, para os testes e para as tarefas da DAG.

**A troca de imagem trocou o uid, e diretório antigo ficou ilegível.** A imagem
`apache/spark` roda como uid 185; a do Airflow, como 50000. Diretórios de dados criados
pela imagem anterior ficaram `drwxr-xr-x 185:185` e o novo usuário não conseguia escrever
neles — `Mkdirs failed to create`. O conserto não foi apagar e seguir: os fixtures de teste
passaram a **criar as raízes pelo host**, onde o bind mount as expõe com permissão
permissiva. Apagar resolveria hoje e voltaria na próxima troca de imagem.

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
- A imagem carrega Java 17 e pesa centenas de megabytes. É custo de disco, pago uma vez.
- Quem clona não instala Java, Spark nem `winutils`. Só Docker.

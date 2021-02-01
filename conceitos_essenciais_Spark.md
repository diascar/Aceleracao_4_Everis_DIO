# Conceitos importantes para entender o Apache Spark

Vimos nas últimas aulas que o Spark é uma ferramenta de alto desempenho fundamental para quem trabalha com Big Data. Trata-se de uma "engine computacional e um conjunto de bibliotecas para processamento paralelo dedados". Pode ser empregado em uma grande variedade de tarefas desde a ingestão até a análise dos dados.

Neste tópico, gostaria de trazer as definições dos conceitos discutidos em aula e que são essenciais para que possamos entender como o Spark funciona.


## Drivers e Executors

Vamos começar com uma visão geral sobre as **aplicações Spark**. Elas são compostas de dois tipos de processos principais: **driver** e **executors**.

* O *driver* atua como um coordenador do trabalho que será desempenhado pelos *executors*. Para tanto, é ele que roda o seu programa (escrito em Python, Scala ou R) e distribui as tarefas entre os executores.
* Os *executors* são os responsáveis pelo trabalho, afinal, eles executam o código atribuído a eles pelo *driver*.

Outro ponto importante sobre o *driver* é que ele é repsonsável por criar um objeto **SparkSession**, que é o "principal ponto de entrada para as funcionalidades do Spark, representando a conexão da sua aplicação com o cluster Spark. Quando rodamos o Spark no modo interativo, via spark-shell ou PySpark, o *SparkSession* é criado automaticamente. Note que o *SparkSession* unifica outros contextos usados em versões anteriores ao Spark 2.

No PySpark, podemos acessar esses objetos da seguinte maneira:

~~~python
# Veriricando o SparkContext (criado internamente pela SparkSession)
SparkContext
#<class 'pyspark.context.SparkContext'>

# Verificando o SparkSession
spark
#<pyspark.sql.session.SparkSession object at 0x7fd2ea267d50>

# Verificando o SQLCOntext - obsoleto.
SQLCOntext
~~~


Por outro lado, quando escrevemos uma aplicação que não será executada de modo interativo, devemos instanciar um objeto *SparkSession* em nosso código.

~~~python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("yarn") \
    .appName("Nome_da_aplicação") \
    .getOrCreate()
~~~

* `.master("local[n]")`: o Spark rodará na máquina local fazendo uso de *n* threads. Uma outra possibilidade é rodar o Spark em "cluster mode" e usar, por exemplo, o YARN, como cluster manager: `.master("yarn")`.
* `.appName("app_name")`: define o nome da aplicação (nome este que será exibido na inferface web do Spark).
* `getOrCreate()`: Obtém uma SparkSession ou, caso não exista, cria uma nova.


## DataFrames

Agora estamos prontos para carregar e manipular um **DataFrame** em nossa aplicação. E aqui nos deparamos com um conceito familiar (para quem está acostumado com o Pandas ou R). Um *DataFrame*, API para lidar com dados estruturados, é "uma coleção distribuída de dados agrupados em colunas nomeadas". Em outras palavras, diferentemente do que vemos no Pandas e no R, onde os dataframes estão localizados, geralmente, em uma única máquina, um *DataFrame* do Spark está dividido em pedaços distribuídos no cluster. A cada um desses "pedaços" damos o nome de **partição**. E é essa divisão em *partições* que permite que os *executors* realizem seu trabalho em paralelo.


~~~python
df = spark.read.csv("avengers.csv", header = True, inferSchema = True)

# ou
df = spark.read.option("inferSchema", "true").option("header", "true").csv("avengers.csv")

# exibindo as 10 primeiras linhas
df.show(10)

# contando o número de linhas
df.count()
#173

# aplicando um filtro e contando o número de linhas
AppearencesMaior100 = df.where("Appearances > 100")
~~~

Note que spark, no código acima, faz referência ao SparkSession instanciado quando iniciamos o shell do PySpark.


## Transformações, ações e planos

Caso esteja replicando os comandos acima no shell do PySpark, você terá notado que o último comando (`df.where("Appearances > 100")`) não apresenta nenhuma saída. Isso acontece porque, embora tenhamos aplicado uma **transformação** ao nosso dataframe, ela só será de fato aplicada quando executarmos uma **ação** sobre os dados. Uma **ação**, nesse contexto, é uma instrução para que o Spark compute o resultado de uma ou mais transformações. Ações podem incluir: a exibição dos dados no terminal, a coleta dos dados em um objeto nativo do python (uma lista ou um dicionário, por exemplo), ou a escrita dos dados para um arquivo.
Mas como o Spark sabe quais transformações aplicar quando uma ação é executada? Ele cria **planos de execução** (lógico e físico) nos quais são registradas todas as sequências de transformações que serão aplicadas aos dados. Detalhes sobre como o Spark cria e otimiza os planos de execução podem ser lidos [aqui](https://blog.knoldus.com/understanding-sparks-logical-and-physical-plan-in-laymans-term/).

Para exibir o plano físico, para fins de depuração do código, podemos usar o método `.explain()`. Caso queiramos também inspecionar os planos lógicos, basta passar `True` como argumento do método (`.explain(True)`)

~~~python
AppearencesMaior100.explain() # use .explain(True) para visualizar o plano lógico.

#== Physical Plan ==
#*(1) Project [URL#10, Name/Alias#11, Appearances#12, Current?#13, Gender#14, Probationary Introl#15, Full/Reserve Avengers Intro#16, #Year#17, Years since joining#18, Honorary#19, Death1#20, Return1#21, Death2#22, Return2#23, Death3#24, Return3#25, Death4#26, #Return4#27, Death5#28, Return5#29, Notes#30]
#+- *(1) Filter (isnotnull(Appearances#12) && (Appearances#12 > 100))
#   +- *(1) FileScan csv [URL#10,Name/Alias#11,Appearances#12,Current?#13,Gender#14,Probationary Introl#15,Full/Reserve Avengers #Intro#16,Year#17,Years since joining#18,Honorary#19,Death1#20,Return1#21,Death2#22,Return2#23,Death3#24,Return3#25,Death4#26,#Return4#27,Death5#28,Return5#29,Notes#30] Batched: false, Format: CSV, Location: InMemoryFileIndex[hdfs://bigdata-srv:8020/user/#everis/avengers.csv], PartitionFilters: [], PushedFilters: [IsNotNull(Appearances), GreaterThan(Appearances,100)], ReadSchema: #struct<URL:string,Name/Alias:string,Appearances:int,Current?:string,Gender:string,Probationary In...
~~~

Podemos executar todas as transformações planejadas em nosso DataFrame *AppearencesMaior100* exibindo o número de registros recuperados após a filtragem, ou podemos coletar os dois primeiros registros em uma lista.

~~~python
AppearencesMaior100.count()
# 103

# coletando os dois primeiros registros do DataFrame 
AppearencesMaior100.take(2)
~~~


## RDDs

Além dos DataFrames, o Spark conta com outras estruturas de dados (e APIs), tais como: **RDDs** e **Datasets**. Destas, a estrutura mais fundamental, sobre a qual as demais se "apoiam" é representada pelas RDDs (*Resilient Distributed Datasets*). Assim como os DataFrames, as RDDs são coleções de dados distribuídos, contudo, representam estruturas de mais baixo nível e armazenam objetos da linguagem escolhida pelo programador. Embora as RDDs tenham suas aplicações, na maior parte dos casos, recomenda-se o uso de estruturas como DataFrames ou Datasets.



# Referências

* Spark,: The Definitive Guide
* https://spark.apache.org/
* https://medium.com/@achilleus/spark-session-10d0d66d1d24
* https://data-flair.training/blogs/* learn-apache-spark-sparkcontext/
* https://sparkbyexamples.com/pyspark/* pyspark-what-is-sparksession/
* https://blog.knoldus.com/* understanding-sparks-logical-and-physical-plan-in-laymans-term/

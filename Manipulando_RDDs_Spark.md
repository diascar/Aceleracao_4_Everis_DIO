# Apache Spark - manipulando RDDs

Vimos em um tópico anterior sobre o Apache Spark que as RDDs (Resilient Distributed Datasets) são as estruturas de dados fundamentais do Spark. Vimos ainda como elas contribuem para que o Spark seja uma ferramenta muito eficiente no processamento de grandes volumes de dados. Algumas de suas principais características são:
* Computação na memória.
* Imutabilidade.
* Particionamento.
* Tolerância a falhas.

Neste tópico, vamos discutir alguns comandos para a criação e manipulação de RDDs usando o *PySpark*.

Vale lembrar que, nos comandos abaixo, `sc` se refere ao SparkContext (que representa seu ponto de conexão com um cluster Spark).

### Criando RDDs a partir de coleções

Uma forma de criar RDDs é a partir da função `sc.parallelize()` que, conforme consta na documentação, permite "distribuir uma coleção (listas, tuplas, etc.) na forma de RDD".

~~~python
lista = list(range(100))

# criando uma RDD a partir da lista acima.
l_rdd = sc.parallelize(lista)
~~~

Acima, dissemos que uma das características das RDDs é o particionamento. Isso quer dizer que os dados são divididos em blocos. A função `sc.parallelize` aceita um segundo argumento que consiste no número de blocos em que os dados serão particionados.
Podemos usar o método `.getNumPartitions()` para acessar o número de partições de uma RDD.

~~~python
# criando uma RDD com 5 partições
l_rdd5 = sc.parallelize(lista, 5)

# acessando o número de partições
l_rdd5.getNumPartitions()
#5

# acessando o número de partições da RDD criada no bloco anterior
l_rdd.getNumPartitions()
~~~

O método `.collect()` retorna um lista com o conteúdo de uma RDD.

~~~python
l_rdd.collect()
#[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]

~~~

Caso queiramos retornar o conteúdo agrupado por partição, podemos usar o método `.glom()`.

~~~python
l_rdd5.glom().collect()
#[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19], [20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39], [40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59], [60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79], [80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]]
~~~

Note que, no exemplo acima, temos listas aninhadas. Cada uma das listas internas, representa uma das (5) partições (que foram definidas quando criamos a RDD).

Diferentemente de outras coleções em Python, não podemos usar a função `len()` para checar o tamanho de uma RDD. Porém, dispomos do método `.count()`, que retorna o número de elementos em uma RDD.

~~~python
l_rdd.count()
#100
~~~

Da mesma forma, não podemos exibir seu conteúdo usando a função `print()` diretamente. Podemos, contudo, usar o método `.foreach()`, que permite a aplicação de uma função a todos os elementos de uma RDD (incluindo a função `print()`).

~~~python
l_rdd.foreach(print)
#0
#1
#2
#3
#4
#5
#(...)
~~~

Caso seja necessário reduzir o número de partições, é possível usar o método `.coalesce()` (preferível ao método `.repartition()` neste caso. [explicação simples](https://stackoverflow.com/questions/31610971/spark-repartition-vs-coalesce)).

~~~python
# Reduzindo para 2 o número de partições
l_rdd1 = l_rdd5.coalesce(2)


l_rdd2.getNumPartitions()
#2
~~~

Também podemos aplicar funções básicas de agregação para o cálculo da soma, média, desvio padrão, etc.

~~~python
l_rdd.sum()
#4950

l_rdd.mean()
#49.5

l_rdd.min()
#0

l_rdd.max()
#99

l_rdd.sampleStdev()
#29.011491975882016
~~~

Uma amostra dos dados em uma RDD pode ser obtida com o método `.sample()` que permite especificar o tipo de amostragem (com ou sem reposição) e a fração do conjunto de dados original que será amostrada.

~~~python
# obtendo uma nova RDD com uma amostra (sem reposição) de 10 % da original
s_rdd = l_rdd.sample(False, 0.1)
s_rdd.collect()
#[3, 7, 13, 17, 22, 25, 32, 66, 79, 89, 90, 94]

s2_rdd = l_rdd.sample(True, 0.05)
s2_rdd.collect()
#[9, 14, 27, 48, 49, 86]
~~~

Também é possível criar uma RDD a partir da união de duas outras existentes. Uma maneira de fazer isso é usando o método `.union()`.

~~~python
u_rdd = s_rdd.union(s2_rdd)
u_rdd.collect()
#[3, 7, 13, 17, 22, 25, 32, 66, 79, 89, 90, 94, 9, 14, 27, 48, 49, 86]
~~~

### Criando RDDs a partir de arquivos de texto

É muito provável que, na maioria dos casos, tenhamos que lidar com dados provenientes de fontes externas. Isso inclui arquivos em em formato texto.
Nesta seção, criaremos uma RDD a partir de um arquivo de texto armazenado no HDFS.

~~~python
# criando uma RDD com o arquivo de texto
pokemon = sc.textFile("saida/part-00000")

# contando o número de "linhas"
pokemon.count()

# exibindo a "primeira linha"
pokemon.first()
# #"104\t84|'Ponyta'|'Fire'|''|50|85|55|65|65|90|1|false"
~~~

Podemos aplicar uma *transformação* a cada elemento da RDD pokemon e salvar o resultado em uma novo objeto do tipo RDD usando o método `.map()`. Vale lembrar que as RDDs são imutáveis e, por isso, transformações aplicadas a elas geram novas RDDs.

~~~python
# extraindo os nomes dos pokemons
pokemon_nomes = pokemon.map(lambda i: i.split('|')[1])
~~~

Podemos contar o número de tipos de pokemons combinando os métodos `.map()` e `.reduceByKey()`.

~~~python
# map
type1 = pokemon.map(lambda i: (i.split('|')[2], 1))

# reduce
type1_count = type1.reduceByKey(lambda x,y: x+y)

# ordenando (ordem decrescente)
type1_count.map(lambda t: (t[1], t[0])).sortByKey(False).take(1)
#[(61, "'Normal'")]

# ordenando (ordem crescente)
type1_count.map(lambda t: (t[1], t[0])).sortByKey(True).take(2)
# [(2, "'Flying'"), (5, "'Steel'")]
~~~

Os pokemons podem ser filtrados de acordo, por exemplo, com seu type1. Para isso, usaremos o método `.filter()`

~~~python
# filtrando os pokemons do tipo Normal (esperamo 61 registros)
Normal = pokemon.filter(lambda line: line.split('|')[2].strip("'") == "Normal"

Normal.count()
#61)

# filtrando os pokemons dos tipos Steel
Steel = pokemon.filter(lambda line: line.split('|')[2].strip("'") == "Steel")

Flying = pokemon.filter(lambda line: line.split('|')[2].strip("'") == "Flying")



# maior HP
pokemon.map(lambda line: int(line.split('|')[4])).max()

# menor HP
pokemon.map(lambda line: int(line.split('|')[4])).min()

# média HP
pokemon.map(lambda line: int(line.split('|')[4])).mean()
~~~

## Transformando RDDs em DataFrames

Uma alternativa, muitas vezes preferível, é criar DataFrames a partir das RDDs (assumindo que os dados são, ou podem ser, estruturados). Podemos fazer essa "conversão" usando o método `.toDF()` ou a função `spark.createDataFrame()`. Em ambos os casos devemos passar, como argumento, um schema, no qual definimos os nomes das colunas e os tipos de dados que elas armazenam.
Podemos deixar que o Spark infira os tipos de dados o que, no nosso caso, fará com que todas as colunas armazenem *Strings*. De outro modo, podemos usar as classes `StructType` e `StructField` para definir o schema de maneira mais precisa. `StructType` recebe três argumentos: nome da coluna, o tipo de dado armazenado na coluna e uma variável booleana indicando se o dado pode ser nulo.


~~~python
# convertendo RDD para DataFrame
# permitindo que os tipos de dados sejam inferidos 
df = pokemon.map(lambda x: x.split('|')[1:]).toDF(('name', \
'type1', 'type2', 'HP', 'Attack', 'Defense', 'SpAtk', 'SpDef', \
'Speed', 'Generation', 'Legendary'))

# exibindo o schema do DataFrame - todos os campos são tratados como string
>>> df.printSchema()
#root
# |-- name: string (nullable = true)
# |-- type1: string (nullable = true)
# |-- type2: string (nullable = true)
# |-- HP: string (nullable = true)
# |-- Attack: string (nullable = true)
# |-- Defense: string (nullable = true)
# |-- SpAtk: string (nullable = true)
# |-- SpDef: string (nullable = true)
# |-- Speed: string (nullable = true)
# |-- Generation: string (nullable = true)
# |-- Legendary: string (nullable = true)


# exibindo os 5 primeiros registros
df.show(5)
#+----------+--------+-----+---+------+-------+-----+-----+-----+----------+---------+
#|      name|   type1|type2| HP|Attack|Defense|SpAtk|SpDef|Speed|Generation|Legendary|
#+----------+--------+-----+---+------+-------+-----+-----+-----+----------+---------+
#|  'Ponyta'|  'Fire'|   ''| 50|    85|     55|   65|   65|   90|         1|    false|
#|'Rapidash'|  'Fire'|   ''| 65|   100|     70|   80|   80|  105|         1|    false|
#|    'Seel'| 'Water'|   ''| 65|    45|     55|   45|   70|   45|         1|    false|
#|  'Grimer'|'Poison'|   ''| 80|    80|     50|   40|   50|   25|         1|    false|
#|     'Muk'|'Poison'|   ''|105|   105|     75|   65|  100|   50|         1|    false|
#+----------+--------+-----+---+------+-------+-----+-----+-----+----------+---------+
#only showing top 5 rows

# criando um DataFrame com os tipos de dados apropriados
# Aqui importaremos apenas os tipos de dados que usaremos
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField, StringType, ShortType

# criando o schema
my_schema = StructType([StructField('name', StringType(), True), 
StructField('type1', StringType(), True), 
StructField('type2', StringType(), True),
StructField('HP', ShortType(), True),
StructField('Attack', ShortType(), True),
StructField('Defense', ShortType(), True),
StructField('SpAtk', ShortType(), True),
StructField('SpDef', ShortType(), True),
StructField('Speed', ShortType(), True),
StructField('Generation', ShortType(), True),
StructField('Legendary', StringType(), True)
])

# aqui convertemos para int os valores numéricos
df = pokemon.map(lambda x: list(map(lambda y: int(y) if y.isdigit() else y.strip("'"), x.split('|')[1:]))).toDF(my_schema)
df.printSchema()
#root
# |-- name: string (nullable = true)
# |-- type1: string (nullable = true)
# |-- type2: string (nullable = true)
# |-- HP: short (nullable = true)
# |-- Attack: short (nullable = true)
# |-- Defense: short (nullable = true)
# |-- SpAtk: short (nullable = true)
# |-- SpDef: short (nullable = true)
# |-- Speed: short (nullable = true)
# |-- Generation: short (nullable = true)
# |-- Legendary: string (nullable = true)

df.show(5)
#+----------+--------+-----+---+------+-------+-----+-----+-----+----------+---------+
#|      name|   type1|type2| HP|Attack|Defense|SpAtk|SpDef|Speed|Generation|Legendary|
#+----------+--------+-----+---+------+-------+-----+-----+-----+----------+---------+
#|  'Ponyta'|  'Fire'|   ''| 50|    85|     55|   65|   65|   90|         1|    false|
#|'Rapidash'|  'Fire'|   ''| 65|   100|     70|   80|   80|  105|         1|    false|
#|    'Seel'| 'Water'|   ''| 65|    45|     55|   45|   70|   45|         1|    false|
#|  'Grimer'|'Poison'|   ''| 80|    80|     50|   40|   50|   25|         1|    false|
#|     'Muk'|'Poison'|   ''|105|   105|     75|   65|  100|   50|         1|    false|
#+----------+--------+-----+---+------+-------+-----+-----+-----+----------+---------+
#only showing top 5 rows
~~~


Com os DataFrames podemos usar o **Spark SQL** e tornar a consulta e a manipulação dos dados processos mais simples. Para isso, devemos primeiro criar uma tabela temporária (que só existirá na spark session atual).

~~~python
# criando uma temporary view a partir do DataFrame
df.createGlobalTempView("pok")
~~~

Uma vez criada a tabela temporária, podemos acessar seu conteúdo usando consultas SQL passadas ao `spark.sql()`. Vale lembrar que consultas a DataFrames usando `spark.SQL` retornam DataFrames.

~~~python
spark.sql("select * from global_temp.pok limit 5").show()
#+----------+--------+-----+---+------+-------+-----+-----+-----+----------+---------+
#|      name|   type1|type2| HP|Attack|Defense|SpAtk|SpDef|Speed|Generation|Legendary|
#+----------+--------+-----+---+------+-------+-----+-----+-----+----------+---------+
#|  'Ponyta'|  'Fire'|   ''| 50|    85|     55|   65|   65|   90|         1|    false|
#|'Rapidash'|  'Fire'|   ''| 65|   100|     70|   80|   80|  105|         1|    false|
#|    'Seel'| 'Water'|   ''| 65|    45|     55|   45|   70|   45|         1|    false|
#|  'Grimer'|'Poison'|   ''| 80|    80|     50|   40|   50|   25|         1|    false|
#|     'Muk'|'Poison'|   ''|105|   105|     75|   65|  100|   50|         1|    false|
#+----------+--------+-----+---+------+-------+-----+-----+-----+----------+---------+


spark.sql("select name from global_temp.pok").show(5)
#+----------+
#|      name|
#+----------+
#|  'Ponyta'|
#|'Rapidash'|
#|    'Seel'|
#|  'Grimer'|
#|     'Muk'|
#+----------+
#only showing top 5 rows

spark.sql("select count(name) as contagem from global_temp.pok").show()
# +--------+
# |contagem|
# +--------+
# |     386|
# +--------+

spark.sql("select name, Generation, Legendary from global_temp.pok \
where legendary = 'true' and generation = 4").show()
#+--------------------+----------+---------+
#|                name|Generation|Legendary|
#+--------------------+----------+---------+
#|              'Uxie'|         4|     true|
#|           'Mesprit'|         4|     true|
#|             'Azelf'|         4|     true|
#|         'Regigigas'|         4|     true|
#|           'Darkrai'|         4|     true|
#|'Shaymin Land Forme'|         4|     true|
#|            'Arceus'|         4|     true|
#+--------------------+----------+---------+

spark.sql("select name from global_temp.pok where name = `'Uxie'`").show()
~~~

<br>


## Algumas referências

* [Spark SQL](https://spark.apache.org/docs/latest/sql-getting-started.html)
* [Spark data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
* [StructType, StructField](https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/)
# MapReduce com Python e Apache Streaming API

O framework *MapReduce* do Hadoop nos permite desenvolver programas capazes de processar (em paralelo) grandes volumes de dados em um ambiente distribuído e tolerante a falhas proporcionado pelo HDFS. Inclusive, algumas das ferramentas que vimos até aqui, como *Sqoop* e *Hive*, fazem uso do framework MapReduce quando executadas.

Embora as bibliotecas desse framework tenham sido desenvolvidas em Java, os programadores **não estão restritos ao uso dessa linguagem** para escrever seus programas para executar tarefas MapReduce. Graças à **API Hadoop Streaming**, podemos usar qualquer linguagem que seja capaz de ler e escrever usando os fluxos de dado padrão (standard input e standard output).

Neste tópico, gostaria de apresentar um script em Python que faz uso da API Hadoop Streaming para executar tarefas de MapReduce.

A ideia é simples:
* processar o arquivo pokemon.sql (aquele que usamos nas aulas sobre o Sqoop),
* substituir as vírgulas por "|",
* filtrar os pokemons de apenas um tipo,
* transferir o resultado para o HDFS.

Não vamos nos conectar ao MySQL (embora isso pudesse ser feito por meio do *mysql.connector* ou pelo Pandas), já que a ideia não é emular o Sqoop, mas apenas discutir o uso do Python para escrever programas de map/reduce.


## Mapper e Reducer

Vamos precisar de dois programas: um *mapper* e um *reducer* (este não é estritamente necessário). O mapper deve retornar, em cada linha, um par chave-valor separados por tabulação (o delimitador pode ser configurado). Em nosso caso:
* o mapper será um script em python que processa as linhas do arquivo pokemon.sql,
* o reducer (que não funcionará como um reducer de fato) será o programa cut, que apenas eliminará as chaves geradas na fase de mapeamento.


### Mapper

~~~python
#!/usr/local/bin/python3.7
import sys
import re

for i,line in enumerate(sys.stdin):
        if line.startswith("("):
                l = re.split("[\(\)]", line.strip())[1].replace(',', '|')
                print(f"{i}\t{l}")
~~~

O código acima deve retornar (386) linhas com o seguinte formato:


775     755|'Swirlix'|'Fairy'|''|62|48|66|59|57|49|6|false


### Reducer


`cut -f 2`

Aqui, estamos apenas selecionando a segunda coluna (ou seja, descartando a chave gerada pelo mapper).


### Rodando o job MapReduce usando o Hadoop Streaming

Uma vez definidos os nossos programas, podemos colocar a mão na massa e rodar o nosso job de MapReduce usando o python.

~~~bash
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar\
-D mapred.reduce.tasks=1 \
-input pokemon.sql -output PastaSaida \
-mapper map.py \
-file /home/everis/sqoop_handson/map.py \
-reducer "cut -f 2"
~~~


**hadoop jar**: com esse comando, o Hadoop se encarrega de incluir todas as bibliotecas Hadoop necessárias.

**-D mapred.reduce.task**: controla o número de mappers. 

**-input**: indicamos o arquivo de entrada (no HDFS).

**-output**: indicamos a pasta de saída (no HDFS).

**-mapper**: indicamos o mapper.

**-file**: indicamos o local do mapper (FS local).

**-reducer**: indicamos o reducer.


Se tudo deu certo (e você pode acompanhar a execução da aplicação usando a interface web do *YARN NodeManager*), o arquivo processado estará disponível (no HDFS) na pasta de saída indicada durante a execução.

Existem várias formas de ajustar as suas tarefas de Map/Reduce: Você pode controlar o número de mappers, reducers, escolhar e vai compactar ou não a saída, etc.


## Contando palavras

Agora que já sabemos como montar um job de MapReduce usando o Hadoop Stream, vamos recriar o famoso contador de palavras em python.

### Mapper

~~~python
#!/usr/bin/env python3.7

import sys
import re


for line in sys.stdin:
    words = line.strip().split()
    for word in words:
        if word:
            word = re.sub("[,\.\?;:!]+", "", word.lower())
            print(f"{word}\t{1}")
~~~

### Reducer

~~~python
#!/usr/bin/env python3.7

import sys

prev_word = None
total = 0

for line in sys.stdin:
    (word, count) = line.strip().split("\t")
    if (prev_word) and (prev_word != word):
        print(f"{prev_word}\t{total}")
        (prev_word, total) = (word, int(count))
    else:
        (prev_word, total) = (word, total + int(count))

if prev_word:
    print(f"{prev_word}\t{total}")
~~~

### Preparando e rodando o contador de palavras


~~~Bash
# copiando o arquivo para o HDFS
hdfs dfs -copyFromLocal text.txt WordCounterDir/text.txt

# rodando o job de MapReduce
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar\
-D mapred.reduce.tasks=4 \
-input text.txt -output WordCounterDir/resultados \
-mapper map.py \
-file /home/map.py \
-reducer reduce.py
-file /home/reduce.py
~~~

Pronto! É possível conferir os resultados usando esta ferramenta [online](https://www.wordcounttool.com/).

## Referências

* Hadoop: The definitive Guide
* Hadoop Stream - http://hadoop.apache.org/docs/r1.2.1/streaming.html
* Standar streams of data - http://www.linfo.org/standard_input.html
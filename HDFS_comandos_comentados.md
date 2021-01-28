# HDFS: Comandos comentados

## ativando o Namenode

* Nó de um cluster HDFS responsável por gerenciar o sistema de arquivos e regular o acesso dos clientes aos arquivos. Ele é o responsável operações como abrir, fechar e renomear arquivos ou pastas.

`sudo service hadoop-hdfs-namenode start`


## ativando o Namenode secundário

`sudo service hadoop-hdfs-secondarynamenode start`


## ativando o datanode

* Os DataNodes são responsáveis por gerenciar o aramazenamento do nó em que se encontram. Afinal, neles os dados estão fisicamente armazenados.

`sudo service hadoop-hdfs-datanode start`


## Ativando MapReduce History Server:
* permite ao usuário obter informações sobre aplicações concluídas)
sudo service hadoop-mapreduce-historyserver start

## Ativando o YARN
O YARN separa as tarefas de gerenciamento de recursos e agendamento de jobs em dois processos distintos (daemons): ResourceManager e NodeManager

## Ativando o YARN ResourceManager
* gerencia o recurso que será distribuído **entre todas as aplicações do sistema**.

`sudo service hadoop-yarn-resource-manager start`


## Ativando o YARN NodeManager
* é o framework responsável pelos "conteiners" e por gerenciar recursos **por máquina**, monitorandoo os recursos usados e reportando o ResourceManager.

`sudo service hadoop-yarn-nodemanager start`


## Comandos HDFS
Uma vez que todos os serviços já tenham sido iniciados, estamos prontos para brincar com o framework Hadoop.

### vamos começar criando um arquivo vazio

`hdfs dfs -touchz arquivo1.txt`

Podemos listar o conteúdo da pasta atual com o comando `ls` e verificar o arquivo criado, certo? Errado! Isso acontece porque os comandos iniciados por `hdfs dfs` permitem que possamos interagir diretamento com o HDFS. Desse modo, o arquivo1.txt foi criado no diretório de trabalho atual do HDFS (/user/<username>). Vamos conferir?

`hdfs dfs -ls`

ou

`hdfs dfs -ls arquivo1.txt`

O último comando irá exibir na tela o seguinte conteúdo:

`-rw-r--r--   3 usuario grupo       0 2021-01-20 07:22 texto.txt`

O número 3 (na coluna entre as permissões e o nome do usuário) indica o **fator de replicação** do arquivo (isso significa que existem 3 cópias de cada bloco deste arquivo).

Após executar os comandos `ls` (lista os arquivos no sistema de arquivos local) e `hdfs dfs -ls` (lista os arquivos no HDFS) e perceber que eles possuem saídas distintas, fica fácil perceber que o HDFS e os sistema de arquivos local coexistem em uma mesma máquina. Além disso, podemos concluir que o arquivo criado no HDFS não existe como tal na máquina local. Para visualizar melhor esse conceito, basta lembrar que um arquivo no HDFS existe como um conjunto de blocos de igual tamanho (com exceção do último bloco) armazenados em diferentes nós de um cluster Hadoop, e só o Namenode tem (na sua memória) as informações necessárias para organizá-los em um arquivo novamente.

Certo, mas onde são armazenados os blocos? No sistema de arquivos local! Essa informação pode ser encontrada nos arquivos de configuração do Hadoop (lá na pasta /etc), partiuclarmente, no arquivo *hdfs-site.xml*: (basta conferir as seguintes propriedades dfs.namenode.name.dir e dfs.datanode.data.dir).

## Outros comandos úteis

`hdfs dfs -mkdir test-folder`

Podemos criar um arquivo localmente e enviá-lo para a pasta que acabmos de criar no HDFS.

`touch arquivo2.txt` -  cria arquivo no FS local

`hdfs dfs -copyFromLocal arquivo2.txt test-folder`

`hdfs dfs -ls test-folder`

* O comando `-copyFromLocal` funciona como o `-put`, porém, o arquivo de origem deve ser do FS local.

* Vamos apagar o arquivo2.txt, localmente, e copiá-lo novamente, mas desta vez, a partir do HDFS

rm arquivo2.txt

`hdfs dfs -copyToLocal test-folder/arquivo2.txt .`

* O comando `-copyToLocal` funciona como o `-get`, porém, o destino deve ser o FS local.

## Definindo o fator de replicação

`hdfs dfs Ddfs.replication=2 -copyFromLocal arquivo2.txt test-folder`
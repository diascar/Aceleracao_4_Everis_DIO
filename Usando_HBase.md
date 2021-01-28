# Usando o HBase

Trago, neste tópico, as *minhas respostas* para os exercícios (sobre o HBase ) deixados pelos professor Valdir Sevaios. Digo que são *minhas respostas*, pois acredito que deve haver várias (e melhores) formas de responder a essas questões. De qualquer modo, acho que vale a pena trazer para que possamos discutir e pensar em alternativas.


# 1 Criar uma tabela que representa lista de Cidades ...


Em primeiro lugar, vamos acessar o shell do HBase.


`hbase shell`

Podemos criar a tabela *cidades* com os campos pedidos:


`create 'cidades', {NAME=>'info', VERSIONS=>5}, {NAME=>'responsaveis', VERSIONS=>5}, {NAME=>'estatisticas', VERSIONS=>5}`


Conferindo se a tabela *cidades* foi criada com sucesso:

`describe 'cidades'`


# 2 Inserir 10 cidades na tabela criada de cidades

Vamos usar o comando put para inserir os dados para a primeira cidade.

```
put 'cidades', '3106200', 'info:nome', 'Belo Horizonte'
put 'cidades', '3106200', 'info:fundacao', '1897'
put 'cidades', '3106200', 'responsaveis:prefeito', 'Alexandre Kalil'
put 'cidades', '3106200', 'responsaveis:posse', '01012021'
put 'cidades', '3106200', 'responsaveis:vice', 'Fuad Noman'
put 'cidades', '3106200', 'estatisticas:eleicao', '15112020'
put 'cidades', '3106200', 'estatisticas:populacao', '2522000'
put 'cidades', '3106200', 'estatisticas:nEleit', '1956410'
put 'cidades', '3106200', 'estatisticas:anoFundacao', '1897'
```

Já deu pra perceber que não é uma boa ideia inserir todos os dados de todas as cidades à mão. Por isso, vamos salvar os dados das demais cidades em um arquivo csv e importá-lo para o HBase.


Uma vez criado o arquivo *cidades.csv*, precisamos enviá-lo para o HDFS (especificamente, na pasta hbase-test):


`hdfs dfs -copyFromLocal cidades.csv hbase-test`


Agora é só importar importar para o HBase

```
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
-Dimporttsv.separator=',' \
-Dimporttsv.columns='HBASE_ROW_KEY,info:nome,info:fundacao,responsaveis:prefeito,responsaveis:posse,responsaveis:vice,estatisticas:eleicao,estatisticas:populacao,estatisticas:nEleit,estatisticas:anoFundacao' \
cidades hbase-test/cidades.csv
```

Veja que, no comando acima, especificamos o delimitador (`-Dimporttsv.separator=','`), a tabela onde serão inseridos os dados (`cidades`) e o arquivo a partir do qual os dados serão importados (`hbase-test/cidades.csv`).


# 3 Realizar uma contagem de linhas na tabela


`count 'cidades'`

# 4 Consultar só o código e nome da cidade

`scan 'cidades', {COLUMNS=>'info:nome'}`


Aqui, usamos o comando `scan`, especificamos a tabela `cidades` e o par column-family:column-qualifier `info:nome` que contém o nome da cidade. Por padrão, o código da cidade (*row-key*) será exibido.


# 5 Escolha uma cidade, consulte os dados dessa cidade em específico

Retornando todos os dados (colunas) para a cidade de Manaus (cujo código é *1302603*).

`get 'cidades', '1302603'`


Retornando apenas o nome da cidade

`get 'cidades', '1302603', {COLUMNS=>'info:nome'}`


Retornando apenas a população

`get 'cidades', '1302603', {COLUMNS=>'estatisticas:populacao'}`


Retornando apenas o nome do prefeito e do vice:


`get 'cidades', '1302603', {COLUMNS=>['responsaveis:prefeito', 'responsaveis:vice']}`



# 6 Altere para a cidade escolhida os dados de Prefeito, Vice Prefeito e nova data de Posse

```
put 'cidades', '1302603', 'responsaveis:prefeito', 'NOVO PREFEITO'
put 'cidades', '1302603', 'responsaveis:vice', 'VICE (NOVO)'
put 'cidades', '1302603', 'responsaveis:posse', '31012021'
```

Mais uma vez usamos o comando `put`, que permite inserir e atualizar os dados de uma coluna.


# 7 Consulte os dados da cidade alterada


`get 'cidades', '1302603'`


# 8 Consulte todas as versões dos dados da cidade alterada


`get 'cidades', '1302603', {COLUMN=>'responsaveis', VERSIONS=>5}`


Para checar até o número máximo de versões (que definimos durante a criação da tabela), usamos o modificador `VERSIONS` e o número de versões que queremos exibir.

# 9 Exclua as três cidades com menor quantidade de habitantes e quantidade de eleitores

Podemos resolver esse exercício de diferentes maneiras. A maneira mais simples é olhar a tabela, encontrar as 3 cidades com as menores populações, coletar suas respectivas row-keys e exclui-las da tabela. De outro modo, poderíamos usar o HIVE ou recompilar o HBase aplicando os patches que incluem funções de agregação. Achei mais conveniente usar shell script para resolver o problema da ordenação e recuperação das row-keys (ele poderá ser reutilizado nas questões seguintes).

```bash
#!/usr/bin/bash
#$1 - numero de registros para retornar
#$2 - arquivo de saída
#$3 - tipo de sort (r para reverso)
tail -n +7 |
head -n -2 |
cut -f 2,5 -d' ' |
cut -f 1-2 -d'=' --output-delimiter=' '|
sort -$3nk3 |
head -n $1 |
cut -f 1 -d ' ' > $2
```

O script acima recebe o resultado de uma consulta do HBase, ordena (em ordem crescente ou decrescente) e salva as N row-keys em um arquivo. Ele recebe 3 argumentos posicionais: 

* o número de row-keys que serão exportadas.
* o nome do arquivo de saída.
* (opcional) tipo de ordenação (crescente ou decrescente). [r pra decrescente]


```bash
echo "scan 'cidades', {COLUMNS=>'estatisticas:populacao'}" \
| hbase shell | ./sort_query.sh 3 row-keys.txt
```

Ao rodar o comando acima, teremos um arquivo row-keys.txt, contendo as row-keys das 3 cidades com as menores populações. Agora precisamos excluir os registros baseados nas row-keys salvas nesse arquivo. Para isso, vamos usar outro shell script para executar os comandos em batch.

```bash
#!/usr/bin/bash
#$1 arquivo com as row-keys
#$2 comando hbase
while read -r line;
do 
   echo "$2 'cidades', '$line' $3"; 
done < $1 | hbase shell
```

Executando os comandos em batch:

`./batch_process.sh newkeys.txt deleteall`


O script recebe dois argumentos posicionais:
* o nome do arquivo contendo as row-keys.
* o comando HBase que será executado.


# 10 Liste todas as cidades novamente


`scan 'cidades'`


Se tudo deu certo até aqui, o HBase informará que encontrou **7 registros**.


# 11 Adicione na ColumnFamily “estatísticas”, duas novas colunas de “quantidade de partidos políticos” e “Valor em Reais à partidos” para as 2 cidades mais populosas cadastradas

```bash
echo "scan 'cidades', {COLUMNS=>'estatisticas:populacao'}" | \
hbase shell | ./sort_query.sh 3 row-keys2.txt r
```

Usaremos nosso script novamente, mas desta vez, incluiremos o argumento `r`, para que os registros sejam ordenados em ordem decrescente.

```bash
./batch_process.sh row-keys2.txt put ", 'estatisticas:qtdePartidos', 'X'"

./batch_process.sh row-keys2.txt put ", 'estatisticas:valorPartidos', 'Y'"
```

Mais uma vez, processamos em batch. Aqui vamos incluir os mesmos valores para as 3 cidades, mas poderíamos preparar mais um script para lidar com isso.



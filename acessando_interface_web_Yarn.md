# Acessando o YARN Resource Manager usando uma interface web

Nós, entusiastas da tenologia, estamos acostumados a usar o terminal para manipular arquivos e gerenciar tarefas. Mas nem sempre investigar a saída de comandos ou vascular arquivos de log no terminal é a tarefa mais agradável. Uma alternativa, quando se trata do framework Hadoop, é usar a interface web para acessar *YARN ResourceManager*. Com ela podemos ter acesso a:

- informações gerais sobre o cluster HDFS (estado dos nós, bem como seu endereço e memória disponível, etc.).
- informações gerais sobre as aplicações enviadas (quantas estão rodando, número de containers sendo utilizados, uso de memória, etc.).
- informações detalhadas sobre cada uma das tarefas submetidas, sendo possível também a filtragem das tarefas por estado (submetidas, aceitas, concluídas, interrompidas, etc.).

Ok! Parece interessante. Mas como a interface web do Nodemanager a partir do navegador?
Por padrão, ela está acessível no endereço *http://localhost:8088/*
Contudo, no nosso caso, esse endereço não irá funcionar por dois motivos:
1. **localhost** ira redirecionar para a máquina remota a partir da qual acessamos o cluster HDFS.
2. Na nossa máquina virtual, as configurações foram alteradas e a web interface não está disponível no enderço padrão.

Como saber qual é o endereço correto? Basta verificar a propriedade **yarn.nodemanager.hostname** no arquivo de configuração  *yarn-site.xml*. Para conferir o valor definido para esta propriedade, podemos usar o seguinte comando:

`grep -h -A 2 -B 1 'yarn.nodemanager.hostname' /etc/hadoop/conf/yarn-site.xml`

Com base na saída do comando, concluímos que o endereço então é: **bigdata-srv**. Mas que endereço é esse??
Como se trata de um nome (e não um IP), podemos recorrer ao arquivo */etc/hosts*, cuja finalidade, dentre outras coisas é permitir o mapeamento de um nome (bigdata-srv, por exmeplo) a um IP (que vai variar de acordo com sua máquina). Vamos conferir então, qual é o enderço IP associado ao nome bigdata-srv?

`grep bigdata-srv /etc/hosts`

Ótimo, agora estamos quase prontos para poder acessar o *ResourceManager* pelo navegador. Antes, porém, será necessário ajustar as configurações do firewall. Como se trata de um ambiente de testes, podemos simplesmente desabilitar o firewall (maneira mais simples).

`sudo service firewalld stop`

Caso queiramos uma abordagem um pouco mais complicada, porém, mais segura, podemos definir uma regra e dar acesso à nossa máquina.

```bash
sudo firewall-cmd --zone=public --add-service=http
sudo firewall-cmd --zone=public --add-rich-rule 'rule family="ipv4" source address=192.168.X.X port port=8088 protocol=tcp accept'
```

No segundo comando acima, substitua *192.168.X.X* pelo endereço da sua máquina local.
Estamos prontos para acessar a interface web do *ResourceManager*, basta colocar no seu navegador o enderço *192.168.Y.Y:8088* (substituindo o 192.168.Y.Y pelo endereço IP da máquina virtual).


# Referências

* além da documentação específica do YARN, esse tópico foi baseado no conteúdo da seguinte página: https://www.linode.com/docs/guides/introduction-to-firewalld-on-centos/
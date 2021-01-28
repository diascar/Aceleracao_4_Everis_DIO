# Exercício 2 [386 registros recuperados]

## SQL
`SELECT * FROM trainning.pokemon WHERE Type2 = '';`

## Scoop import
```
sudo -u hdfs sqoop import \
--connect jdbc:mysql://localhost/trainning \
--username root --password "Everis@2021" \
--fields-terminated-by "|" \
--split-by Generation \
--target-dir /user/everis-bigdata/pokemon/exerc2 \
--table pokemon \
--where "Type2 = ''" \
--compress \
--num-mappers 4
```

# Exercício 3

## SQL
`SELECT * FROM trainning.pokemon ORDER BY Speed DESC LIMIT 10;`


## Scoop import - utilizando apenas 1 mapper

```
sudo -u hdfs sqoop import \
--connect jdbc:mysql://localhost/trainning \
--username root --password "Everis@2021" \
--fields-terminated-by "|" \
--split-by Number \
--target-dir /user/everis-bigdata/pokemon/exerc3 \
--query 'SELECT * FROM pokemon WHERE $CONDITIONS order by Speed desc limit 10' \
--compress \
--num-mappers 1
```


# Exercício 4

# SQL
`SELECT * from pokemon ORDER BY HP limit 50;`

## Scoop import - utilizando apenas 1 mapper

```
sudo -u hdfs sqoop import \
--connect jdbc:mysql://localhost/trainning \
--username root --password "Everis@2021" \
--fields-terminated-by "|" \
--split-by Number \
--target-dir /user/everis-bigdata/pokemon/exerc4 \
--query 'SELECT * FROM pokemon WHERE $CONDITIONS order by HP limit 50' \
--compress \
--num-mappers 1
```

# Exercício 5

## SQL
`SELECT *, SUM(HP+Attack+Defense+SpAtk+SpDef+Speed) AS Total FROM pokemon GROUP BY Number ORDER BY Total DESC LIMIT 100;`

## Scoop import - utilizando apenas 1 mapper

```
sudo -u hdfs sqoop import \
--connect jdbc:mysql://localhost/trainning \
--username root --password "Everis@2021" \
--fields-terminated-by "|" \
--split-by Number \
--target-dir /user/everis-bigdata/pokemon/exerc5 \
--query 'SELECT *, SUM(HP+Attack+Defense+SpAtk+SpDef+Speed) AS Total FROM pokemon WHERE $CONDITIONS GROUP BY Number ORDER BY Total DESC LIMIT 100' \
--compress \
--num-mappers 1
```
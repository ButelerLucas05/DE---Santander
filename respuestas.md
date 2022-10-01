
## Pregunta 1

En mi opinion se podria resolver esta peticion utilizando Spark , para ejecutar las consultas SQL sobre el dataframe que contenga la tabla principal,
por razones de tamaño, se deberia ir trabajando sobre particiones de la tabla principal e ir creando mediante un proceso de upsert las tablas relacionales necesarias,
Utilizaria Spark principalmente por la velocidad de procesamiento y paralelismo que ofrece.

## Pregunta 2

Al momento de realizar ingestas de datos de alto volumen, se debe prestar atencion a las variables de Spark que hacen referencia 
a la memoria y CPU que se le designa a cada Executor algunos de los parametros que podemos setear son :

- spark.executor.instances: Numero de executor que tendra la aplicacion

- spark.executor.memory: Memoria de cada executor

- spark.executor.cores: Numero de cores que tendra cada excecutor, esto permite paralelizar las task que se envien a este executor

- spark.driver.memory: Memoria que tendra el driver

- spark.driver.cores: Numero de cores virtuales que tendra el driver

- spark.sql.shuffle.partitions: Numero de particiones que se van a usar para mezclar datos al momento de hacer joins o agregaciones

- spark.default.parallelism:  Numero de particiones en RDDs que devuelven las transformaciones 
 

Al momento de escribir lo haria en formato parquet, ya que esta optimizado para grandes cantidades de datos y usaria como metodo de compresion Snappy,
elegiria este formato, ya que no utiliza tanto CPU.
por lo que el costo beneficio de  usar menos CPU a cambio de mas almacenamiento es favorable en entornos Cloud


## Pregunta 3

Probablemte se podria agregar algun data quality check como puede ser la libreria del protyecto Great Expectations para verificar la calidad de los datos.


## SQL:

### 1 - Tabla first_login: Esta tabla contiene el primer login de cada usuario a la aplicacion

### consideraciones : Asumiendo que el event_description del login sea 'LOGIN' y asumiento que la base es postgresql

WITH dataset as (
SELECT
    user_id,
    event_id,
    event_description,
    server_time,
    rank() OVER (PARTITION BY user_id ORDER BY server_time ASC)
FROM tabla_principal tp 
WHERE tp.event_description = 'LOGIN'
)
SELECT 
    user_id,
    event_id,
    event_description,
    server_time
FROM dataset
WHERE dataset.rank = 1


## 2- Tabla actividad_diaria:

Teniendo en cuenta que no realice el modelado por varias dudas al momento de ver los campos, entiendo que la tabla principal tal cual esta armada muestra el historial de actividad de cada usuario y solo seria necesario filtrar por la fecha requerida.

## 3 - KPI retencion de clientes:

No me quedaron del todo claras las condiciones para calcular el KPI,
1 - los dias consecutivos pueden ser de cualquier momento ?, o bien hay que tomar los ultimos 2 dias?
2 - La duracion de la sesión es la suma de todas las sesiones ?, o bien con que una sesión supere los 5 minutos ya es valido?

Para el calculo del KPI seguiria los siguientes pasos:

1 - Obtener el numero total de clientes.
2 - Obtener el numero total de clientes que cumplen las condiciones del KPI.
3 - Obtener el porcentaje de clientes que cumplen con las condiciones con respecto del total.

## Ejercicio 1

Al no resolver el modelado, no pude hacer el DER


## Ejercicio 2

Al no resolver el modelado, no pude hacer las querys,
aunque hubiera creado 3 tablas principales, una de Usuarios , una de Segmentos y otra de Eventos


## Ejercicio 3

### Aclaracion : No entiendo como implementar el KPI de retencion a los 10 clientes que mas veces se hayan logueado, por lo que en esta query solo hice el calculo del top 10 de clientes
### consideraciones : Asumiendo que el event_description del login sea 'LOGIN' y asumiento que la base es postgresql y tomando como referencia el mes calendario en curso

WITH dataset as (
SELECT
    user_id,
    count(*) as cantidad
FROM tabla_principal tp 
WHERE tp.event_description = 'LOGIN' 
AND CAST(tp.server_time AS DATE) BETWEEN CAST('2022-09-01' AS DATE)  AND CAST('2022-09-30' AS DATE)
GROUP BY
tp.user_id
)
SELECT 
    user_id,
    cantidad
FROM dataset
ORDER BY cantidad DESC
LIMIT 10;


### Bonus Track!!!
1 - Se puede utilizar una cola de mensajes para armar procesos de batch, evitando tener una instancia EC2 o un cluster prendido constantemente para ahorrar costos,
    Como lenguaje, no me ataria a uno en particular, se puede construir la solucion con casi cualquier lenguaje cada uno con sus pro y sus contras, aunque para la lectura recomendaria uno que sea compatible con Spark como Python, Java o Scala,  para mejorar la velocidad de procesamiento y aprovechar el paralelismo

2 - Tengo experiencia con Pyspark, principalmente con Databricks como plataforma he creado flujos e ingestas de procesos batch y streaming,
    use esta tecnologia en mi paso por  Etermax, Banco Comafi y Primestone (ejecutando desde Glue)

3 - A lo mejor se podria disponibilizar un endpoint que permita calcular el KPI de retencion de clientes para determinadas fechas,
    o bien se podrian disponibilizar los datos de los eventos o clientes por medio de una API.
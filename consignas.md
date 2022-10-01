# Data Engineer Programming Test

Suponga que se presenta la necesidad de ingestar archivos planos de distintos aplicativos origen, realizarles pequeñas transformaciones (mayoritariamente de formato) y guardarlo en formato Apache Parquet en HDFS.

Dada esta necesidad se pide generar una solución utilizando Pyspark o Spark Scala que contemple las siguientes características:

1.- Debe tener la flexibilidad suficiente para procesar archivos de texto ancho fijo y delimitados.

2.- Como input debe tomar un archivo de configuración que contenga los siguientes parámetros:

	a.- El path de origen del archivo a ser ingestado;
	b.- El path destino donde se escribirá ese archivo;
	c.- La metadata necesaria para procesar el archivo crudo (e.g. delimitador, header, columnas, anchos de columnas);
	d.- La metadata necesaria para generar el archivo en parquet que crea necesarias (e.g. particiones, cantidad de archivos, compresión);
	e.- Permitir el agregado de columnas al data frame con valores predefinidos.
	
Se solicita que lo desarrollado sea capaz de ser ejecutado en cualquier computadora de forma local, de ser posible, con la menor cantidad de dependencias.

### Sets de prueba
Se proveen dos archivos para realizar *pruebas*.

nyse_2012.csv, delimitado por comas, con cabecera. El parquet de salida debe estar particionado por una columna extra, llamada partition_date y cuyo valor viene dado por parámetro.

nyse_2012_ts.csv, de ancho fijo, sin cabecera y con las siguientes longitudes de columnas:

|columna|tipo de dato|longitud|
| ------------- | ------------- | ------------- |
|stock|string|6|
|transaction_date|timestamp|24|
|open_price|float|7|
|close_price|float|7|
|max_price|float|7|
|min_price|float|7|
|variation|float|7|

El parquet de salida debe estar particionado por la columna stock.

### Criterios de evaluación
Tener en cuenta:
* Calidad del código desarrollado.
* Mantenibilidad.
* Claridad y facilidad de entendimiento.
* Performance y escalabilidad.
* Pragmatismo.

(Los criterios de evaluación son guías para brindar mayor claridad al candidato en términos de cómo su trabajo será calificado, esta evaluación se centrará en dichos conceptos pero no necesariamente se limitará a los mismos)


### SQL

Para este test hemos creado un entorno inventado que simula situaciones de nuestra arquitectura actual. El candidato debe suponer que es un empleado del banco Santander y debe resolver una situación planteada por un área de negocio.

En este escenario, los datos de logueo de los usuarios del Santander se encuentran en una única tabla sobre BigQuery. En esta table se guarda toda la información referente a las actividades realizan los usuarios cuando ingresan al Home Banking de Santander. La estructura de dicha tabla se pueden ver a continuación:

![image](https://user-images.githubusercontent.com/62435760/127665003-e3aad47b-616d-44aa-af21-c25249e11123.png)

Basado en esa tabla, el área de la banca privada del banco desea que se arme un modelo dimensional en donde se contemplen dos tablas principales:

●	La primera vez que el usuario se logueo al Home Banking. Asumiendo que dicha situación es un tipo de evento.

●	La actividad diaria de cada usuario.

Tenga en cuenta las siguientes consideraciones técnicas:

1.	La table de origen contiene mil millones de registros.
2.	El modelo a construer debe poder ejecutarse tanto en consolas SQL o en herramientas de BI.
3.	Uno de los KPIs mas importantes que tiene la organización es la de retención de clientes. Dicho KPI es el porcentaje de usuarios que realizaron una actividad diferente al login durante 2 dos días consecutivos y cuya sesión haya durando al menos 5 minutos.

### Pregunta 1
Como resolvería este tipo de petición? Explique detalladamente el proceso de limpieza y transformación del modelo inicial. Que tecnologías utilizaría y por que?

### Ejercicio 1
Realice el DER que de soporte al modelo dimensional solicitado por la banca privada.

### Ejercicio 2 
Escriba las queries necesarias partiendo de la tabla inicial y que de como resultado el modelo planteado en el ejercicio anterior.

### Ejercicio 3
Escriba la consulta necesaria para obtener el KPI de retención de clientes para los 10 clientes que mas veces se hayan logueado en el último mes.

### Pregunta 2
Suponga que la ingesta de estos datos se realiza utilizando Apache Spark debido a que la tabla cruda tiene billones de registros. Que parametros de spark tendría en cuenta a la hora de realizar dicha ingesta? Explique brevemente en que consta cada uno de ellos. 
En que formato de compresión escribiría los resultados? Por que?

### Pregunta 3

Existen varios problemas en cuanto a la calidad de datos de la tablas que consultan los usuarios de la banca privada, se esta investigando como mejorar y prevenir estos incidentes. Describa brevemente que implementaría para garantizar la confiabilidad de los datos.

### Bonus Track!!!
* En qué requerimiento implementarías una cola de mensajes en una solución orientada a datos?  Que lenguaje utilizarías y porque?
* Que experiencia posees sobre py spark o spark scala? Contar breves experiencias, en caso de contar experiencia con otro framework de procesamiento distribuido, dar detalles también.
* Qué funcionalidad podrías expandir desde el area de ingeniería de datos con una API y arquitectónicamente como lo modelarías?
import pyspark
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

class ProcesamientoDeArchivo:
    
    def __init__(self,config_path:str ,partition_value = None):
        self.config_path = config_path
        self.partition_value = partition_value
    
    def get_config(self):
        print(f"Obteniendo config de archivo : {self.config_path}")
        config_dict = {}
        # Agregamos valor de particion
        config_dict["partition_value"] = self.partition_value
        ##Leemos el json de configuracion
        json_file = open(self.config_path)
        config_json = json.load(json_file)

        ## Desarmamos el json de configuracion y armamos un diccionario de configuracion
        try :
            metadata_procesamiento = config_json["metadata_procesamiento"]
            metadata_escritura = config_json["metadata_escritura"]
            config_dict["path_origen"] = config_json["path_origen"]
            config_dict["path_destino"] = config_json["path_destino"]
    
            ## Obtenemos datos de metadata procesamiento
            tipo_archivo  = metadata_procesamiento["tipo_archivo"]
            config_dict["tipo_archivo"] = metadata_procesamiento["tipo_archivo"]
            config_dict["header"] = metadata_procesamiento["header"]
    
            if tipo_archivo == "csv":
                config_dict["delimitador"] = metadata_procesamiento["delimitador"]
    
            if tipo_archivo == "txt":
                config_dict["columnas"] = config_json["schema"]
    
            ## Obtenemos datos de metadata de escritura
            config_dict["particion"] = metadata_escritura["particion"]
            config_dict["compresion"] = metadata_escritura["compresion"]
            config_dict["modo_escritura"] = metadata_escritura["modo_escritura"]
            
            ## Obtenemos columnas predefinidas
            config_dict["columnas_predefinidas"] = config_json.get("columnas_predefinidas",[])
            
        except:
            print("Falta algun campo obligatorio en el archivo de configuracion.")
            raise 
        
        return config_dict
    
    def process_file(self,config_dict: dict):
        
        ## Desarmamos en diccionario de configuracion en variables
        valor_particion = config_dict["partition_value"]
        path_origen     = config_dict["path_origen"]
        path_destino    = config_dict["path_destino"]
        tipo_archivo    = config_dict["tipo_archivo"]
        header          = config_dict["header"]
        modo_escritura  = config_dict["modo_escritura"]
        particion       = config_dict["particion"]
        compresion      = config_dict["compresion"]
        
        columnas_predefinidas = config_dict.get("columnas_predefinidas",[])
        delimitador     = config_dict.get("delimitador",None)
        columnas        = config_dict.get("columnas",[])
        
        ## Lectura y procesamiento en base al tipo de archivo
                        
        if tipo_archivo == "csv":
            print("Procesando CSV...")
            df = spark.read.option("header",str(header)).option("delimiter",delimitador).csv(path_origen)
            df.printSchema()
            
        elif tipo_archivo == 'txt':
            print("Procesando txt...")
            df = spark.read.text(path_origen)
            schema = []
            contador = 0
            inicio = 1
            
            ## Recorremos la lista de columnas para poder hacer los sub str 
            while contador < len(columnas):
                nombre = columnas[contador]["columna"]
                tipo_dato = columnas[contador]["tipo_dato"]
                longitud = int(columnas[contador]["longitud"])
        
                columna = (nombre,inicio,longitud,tipo_dato)
                inicio = inicio + longitud
                schema.append(columna)
        
                contador = contador + 1
                
            ## Por cada elemento de la lista en schema, 
            ## creamos una columna nueva con el valor del sub str y casteamos al tipo de dato especificado
            for colinfo in schema:
                df = df.withColumn(colinfo[0], df.value.substr(colinfo[1],colinfo[2]).cast(colinfo[3]))
    
            ## Eliminamos la columna value despues de obtener los substrings
            df = df.drop("value")
            df.printSchema()
        else :
            print(f"ERROR!! tipo de archivo {tipo_archivo} invalido ")
        
        # En caso de haber se agregan las columas con valor predefinido
        for columna in columnas_predefinidas :
                df = df.withColumn(columna["columna"], lit(columna["valor"]))
                print(f'Columna {columna["columna"]} agregada correctamente')
        
        # En caso de definirse la particion con un valor especifico se agrega la columna de particion
        if valor_particion != None:
            df = df.withColumn(particion, lit(valor_particion))
        
        ## Escribimos el archivo
        print(f"Guardando archivo parquet en : {path_destino} , particionado por la columna : {particion} y comprimido en modo: {compresion} ")
        (
            df.write
           .option("compression",compresion)
           .partitionBy(particion)
           .mode(modo_escritura)
           .parquet(path_destino)
        )
        print("Archivo escrito de manera correcta.")



spark = SparkSession.builder.appName("test").getOrCreate()

################################ Procesamos csv #################################################
procesamiento_csv = ProcesamientoDeArchivo("config_csv.json","2022-09-29")
config_dict =  procesamiento_csv.get_config()
procesamiento_csv.process_file(config_dict)


################################ Procesamos txt #################################################
procesamiento_txt = ProcesamientoDeArchivo("config_txt.json")
config_dict =  procesamiento_txt.get_config()
procesamiento_txt.process_file(config_dict)
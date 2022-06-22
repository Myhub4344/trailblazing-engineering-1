from pyspark.sql.functions import lit
from pyspark.sql import functions
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
import yaml

# Reads Json file and flatten all the nested columns
def flatten_df(nested_df):
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    if not nested_cols:
        arr_cols = [c[0] for c in nested_df.dtypes if c[1][:5] == 'array']
        if not arr_cols:
            return nested_df
        else:
            for x in arr_cols:
               nested_df = nested_df.withColumn(x, explode(col(x)))
            return nested_df
    else: 
        flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
        flat_df=nested_df.select(flat_cols + [col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols for c in nested_df.select(nc+'.*').columns])
        return flatten_df(flat_df)

# Loads multiple tables which mapped in schema file
def table_load(yaml_file,df):
    df_actual_col =df.columns
    documents = yaml.load(open(yaml_file))
    df_all = {}
    for item, doc in documents.items():
        t_col_list=[]                        # Required table column list
        d_col_list=[]                        # Json or DF column list(before rename)
        dt_col = {}                             # mapping of Table and DF column
        d_type = {}                             # Required datatype casting mapping
        for t_d_col in doc.split(" "):
            t_col=t_d_col.split(":")[0]
            d_col=t_d_col.split(":")[1]
            t_col_list.append(t_col)
            (col_name, col_type) = d_col.rstrip('\n').split(".")
            d_col_list.append(col_name)
            dt_col[col_name] = t_col
            d_type[col_name] = col_type
        x = sorted(set(d_col_list) - set(df_actual_col))                    # missing columns in df or json data file
        y = sorted(set(d_col_list).intersection(df_actual_col))             # Available columns in df or json data file
        t_df = df.select(*y)
        if len(x)!=0:
            for c in x:
                t_df = t_df.withColumn(c, lit(None).cast(d_type[c]))  # Adding missing columns as null   
        if len(y)!=0:
            for c in d_col_list:
                t_df = t_df.withColumnRenamed(c,dt_col[c])    # Renaming columns
        #t_df.write.parquet(path/item)        # Writing each table DF to actual table
        df_all[item] = t_df
    return df_all

  #run commands to test code
df = spark.read.option("multiline", "true").json("hdfs://nameservicets1/tmp/complex.json")
df.printSchema()
df5=flatten_df(df)
df5.printSchema()

file1="/home/ag68920/pyspark_exmaples/schema.yml"
ad=table_load(file1,df5)
ad

table1=ad['table1']
table1.printSchema()
table1.show()


>>> df.printSchema()
root
 |-- dc_id: string (nullable = true)
 |-- source: struct (nullable = true)
 |    |-- sensor-igauge: struct (nullable = true)
 |    |    |-- c02_level: long (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- geo: struct (nullable = true)
 |    |    |    |-- lat: double (nullable = true)
 |    |    |    |-- long: double (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- ip: string (nullable = true)
 |    |    |-- temp: long (nullable = true)
 |    |-- sensor-inest: struct (nullable = true)
 |    |    |-- c02_level: long (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- geo: struct (nullable = true)
 |    |    |    |-- lat: double (nullable = true)
 |    |    |    |-- long: double (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- ip: string (nullable = true)
 |    |    |-- temp: long (nullable = true)
 |    |-- sensor-ipad: struct (nullable = true)
 |    |    |-- c02_level: long (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- geo: struct (nullable = true)
 |    |    |    |-- lat: double (nullable = true)
 |    |    |    |-- long: double (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- ip: string (nullable = true)
 |    |    |-- temp: long (nullable = true)
 |    |-- sensor-istick: struct (nullable = true)
 |    |    |-- c02_level: long (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- geo: struct (nullable = true)
 |    |    |    |-- lat: double (nullable = true)
 |    |    |    |-- long: double (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- ip: string (nullable = true)
 |    |    |-- temp: long (nullable = true)

>>> df5=flatten_df(df)
>>> df5.printSchema()
root
 |-- dc_id: string (nullable = true)
 |-- source_sensor-igauge_c02_level: long (nullable = true)
 |-- source_sensor-igauge_description: string (nullable = true)
 |-- source_sensor-igauge_id: long (nullable = true)
 |-- source_sensor-igauge_ip: string (nullable = true)
 |-- source_sensor-igauge_temp: long (nullable = true)
 |-- source_sensor-inest_c02_level: long (nullable = true)
 |-- source_sensor-inest_description: string (nullable = true)
 |-- source_sensor-inest_id: long (nullable = true)
 |-- source_sensor-inest_ip: string (nullable = true)
 |-- source_sensor-inest_temp: long (nullable = true)
 |-- source_sensor-ipad_c02_level: long (nullable = true)
 |-- source_sensor-ipad_description: string (nullable = true)
 |-- source_sensor-ipad_id: long (nullable = true)
 |-- source_sensor-ipad_ip: string (nullable = true)
 |-- source_sensor-ipad_temp: long (nullable = true)
 |-- source_sensor-istick_c02_level: long (nullable = true)
 |-- source_sensor-istick_description: string (nullable = true)
 |-- source_sensor-istick_id: long (nullable = true)
 |-- source_sensor-istick_ip: string (nullable = true)
 |-- source_sensor-istick_temp: long (nullable = true)
 |-- source_sensor-igauge_geo_lat: double (nullable = true)
 |-- source_sensor-igauge_geo_long: double (nullable = true)
 |-- source_sensor-inest_geo_lat: double (nullable = true)
 |-- source_sensor-inest_geo_long: double (nullable = true)
 |-- source_sensor-ipad_geo_lat: double (nullable = true)
 |-- source_sensor-ipad_geo_long: double (nullable = true)
 |-- source_sensor-istick_geo_lat: double (nullable = true)
 |-- source_sensor-istick_geo_long: double (nullable = true)

>>> file1="/home/ag68920/pyspark_exmaples/schema.yml"
>>> ad=table_load(file1,df5)
>>> ad
{'table2': DataFrame[id: bigint, c02_level2: string, temp2: bigint, c02_leve22: bigint, description2: string], 'table1': DataFrame[id: string, c02_level1: bigint, description1: string, id1: bigint, ip1: string, temp1: bigint, c02_level111: bigint, description111: string, id11: bigint, ip11: string, temp11: bigint, c02_level1111: bigint, description1111: string, id_dummmy: int]}
>>> table1=ad['table1']
>>> table1.printSchema()
root
 |-- id: string (nullable = true)
 |-- c02_level1: long (nullable = true)
 |-- description1: string (nullable = true)
 |-- id1: long (nullable = true)
 |-- ip1: string (nullable = true)
 |-- temp1: long (nullable = true)
 |-- c02_level111: long (nullable = true)
 |-- description111: string (nullable = true)
 |-- id11: long (nullable = true)
 |-- ip11: string (nullable = true)
 |-- temp11: long (nullable = true)
 |-- c02_level1111: long (nullable = true)
 |-- description1111: string (nullable = true)
 |-- id_dummmy: integer (nullable = true)

>>> table1.show()
22/06/22 14:21:09 WARN util.Utils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf.
+------+----------+--------------------+---+-----------+-----+------------+--------------------+----+---------------+------+-------------+--------------------+---------+
|    id|c02_level1|        description1|id1|        ip1|temp1|c02_level111|      description111|id11|           ip11|temp11|c02_level1111|     description1111|id_dummmy|
+------+----------+--------------------+---+-----------+-----+------------+--------------------+----+---------------+------+-------------+--------------------+---------+
|dc-101|      1475|Sensor attached t...| 10|68.28.91.22|   35|        1346|Sensor attached t...|   8|208.109.163.218|    40|         1370|Sensor ipad attac...|     null|
+------+----------+--------------------+---+-----------+-----+------------+--------------------+----+---------------+------+-------------+--------------------+---------+

######################    Data Files    ########################################
cat complex.json

{
"dc_id": "dc-101",
"source": {
    "sensor-igauge": {
      "id": 10,
      "ip": "68.28.91.22",
      "description": "Sensor attached to the container ceilings",
      "temp":35,
      "c02_level": 1475,
      "geo": {"lat":38.00, "long":97.00}
    },
    "sensor-ipad": {
      "id": 13,
      "ip": "67.185.72.1",
      "description": "Sensor ipad attached to carbon cylinders",
      "temp": 34,
      "c02_level": 1370,
      "geo": {"lat":47.41, "long":-122.00}
    },
    "sensor-inest": {
      "id": 8,
      "ip": "208.109.163.218",
      "description": "Sensor attached to the factory ceilings",
      "temp": 40,
      "c02_level": 1346,
      "geo": {"lat":33.61, "long":-111.89}
    },
    "sensor-istick": {
      "id": 5,
      "ip": "204.116.105.67",
      "description": "Sensor embedded in exhaust pipes in the ceilings",
      "temp": 40,
      "c02_level": 1574,
      "geo": {"lat":35.93, "long":-85.46}
    }
  }
}



 cat schema.yml
  
table1:
    id:dc_id.string
    id_dummmy:ids.int
    c02_level1:source_sensor-igauge_c02_level.long
    description1:source_sensor-igauge_description.string
    id1:source_sensor-igauge_id.long
    ip1:source_sensor-igauge_ip.string
    temp1:source_sensor-igauge_temp.long
    c02_level111:source_sensor-inest_c02_level.long
    description111:source_sensor-inest_description.string
    id11:source_sensor-inest_id.long
    ip11:source_sensor-inest_ip.string
    temp11:source_sensor-inest_temp.long
    c02_level1111:source_sensor-ipad_c02_level.long
    description1111:source_sensor-ipad_description.string

table2:
    id:source_sensor-ipad_id.long
    c02_level2:source_sensor-ipad_ip.string
    temp2:source_sensor-ipad_temp.long
    c02_leve22:source_sensor-istick_c02_level.long
    description2:source_sensor-istick_description.string







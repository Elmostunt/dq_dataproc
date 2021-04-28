import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from profile_quality import *
from googleapiclient import discovery
import time
import os.path
import os

#############
# Librerias #
#############

#Es donde se ubican las distintas clase y métodos secundarios para el funcionamiento del CCPD.
#Utilidad: Librerias de apoyo para el ccpd.

#-create_table
#    Método estándar para la creación de tablas en Bigquery desde un dataframe.
def create_table(client,dataset_name,table_name,schema):
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,field="date_run",)
    table = client.create_table(table)

#-publish
#    Método estándar para la publicación de tablas en Bigquery desde un dataframe.
def publish(df,client,table_id,schema):
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)


#-publish_metrics_dimensions
#    Método encargado de crear y publicar en bigquery el dataframe proveniente de la función: "split_field".
def publish_metrics_dimensions(df_control,client,dataset_name,destination_table):
    table_id = "{}.{}".format(dataset_name,destination_table)
    schema = [bigquery.SchemaField("tables_bq", "STRING", description='Name of the source table. / Nombre de la tabla de origen.'),
              bigquery.SchemaField("availability", "INTEGER", description='Value that indicates the number of days since the last data update at the time of profiling. / Valor que indica el número de días desde la última actualización de los datos al momento del perfilamiento.'),
              bigquery.SchemaField("epoch", "INTEGER", description='Value in epoch. / Es el valor de tiempo en epoch.'),
              bigquery.SchemaField("date_run", "TIMESTAMP", description='Profiling run datetime. / Corresponde a la fecha y hora de ejecución del perfilamiento.'),
              bigquery.SchemaField("name_field", "STRING", description='Column name. / Nombre de la columna.'),
              bigquery.SchemaField("count_register", "INTEGER", description='Total number of records. / Es el numero total de registros.'),
              bigquery.SchemaField("count_field", "INTEGER", description='Total number of columns. / Es el numero total de columnas.'),
              bigquery.SchemaField("completeness_NaN_count", "INTEGER", description='Total missing data. / Es el número de datos ausentes.'),
              bigquery.SchemaField("completeness_NaN_per", "FLOAT", description='Percentage value of missing data. / El el valor porcentual de datos ausentes.'),
              bigquery.SchemaField("completeness_No_NaN_count", "INTEGER", description='Total non-absent data. / Es el número de datos No ausentes.'),
              bigquery.SchemaField("completeness_No_NaN_per", "FLOAT", description='Percenage value non-absent data. / El el valor porcentual de datos No ausentes.'),
              bigquery.SchemaField("uniqueness_count", "INTEGER", description='Total unique data. / Es la cantidad de datos únicos.'),
              bigquery.SchemaField("uniqueness_per", "FLOAT", description='Porcentage value unique data. / Es el valor porcentual de datos únicos.'),
              bigquery.SchemaField("value_moda", "STRING", description='Most common value in column. / Es valor más recurrente de la columna.'),
              bigquery.SchemaField("count_moda", "INTEGER", description='Number common value_moda. / Es el número de recurencia de value_moda.'),
              bigquery.SchemaField("moda_per", "FLOAT", description='Percentage value of value_data. / Es el valor porcentual de value_moda.')]
    try:
        client.get_table(table_id)
        publish(df_control,client,table_id,schema)
    except NotFound:
        print("tabla {} inexistente, revisar instalación".format(destination_table))

        


#-table_to_list
#    Recibe el input .json y los transforma en listas.
def table_to_list(df):
    name_field = list(df)
    array_project = list(df[name_field[0]])
    array_dataset_name = list(df[name_field[1]])
    array_table_name = list(df[name_field[2]])
    array_destination_dataset = list(df[name_field[3]])
    array_destination_table = list(df[name_field[4]])
    array_sql = list(df[name_field[5]])
    array_specific_value = list(df[name_field[6]])
    array_specific_field = list(df[name_field[7]])
    array_field_validity = list(df[name_field[8]])
    array_type_data = list(df[name_field[9]])
    array_condition = list(df[name_field[10]])
    array_destination_table_validity = list(df[name_field[11]])
    return array_project,array_dataset_name,array_table_name,array_destination_dataset,array_destination_table,array_sql,array_specific_value,array_specific_field,array_field_validity,array_type_data,array_condition,array_destination_table_validity


#-publish_performance
#    Método encargado de crear y publicar en bigquery el dataframe proveniente de la función: "performance_process".
def publish_performance(df,client,dataset_name,destination_table):
    table_id = "{}.{}".format(dataset_name,destination_table)
    schema = [bigquery.SchemaField("table_initial", "STRING", description='Name of the source table. / Nombre de la tabla de origen.'),
              bigquery.SchemaField("count_row", "INTEGER", description='Total numbers of records. / Es el numero total de registros.'),
              bigquery.SchemaField("epoch", "INTEGER", description='Value in epoch. / Es el valor de tiempo en epoch.'),
              bigquery.SchemaField("date_run", "TIMESTAMP", description='Profiling run datetime. / Corresponde a la fecha y hora de ejecución del perfilamiento.'),
              bigquery.SchemaField("availability", "INTEGER", description='Value that indicates the number of days since the last data update at the time of profiling. / Valor que indica el número de días desde la última actualización de los datos al momento del perfilamiento.'),
              bigquery.SchemaField("size_table_KB", "STRING", description='Table weight. / Es el peso de la tabla.'),
              bigquery.SchemaField("key_fields", "STRING", description='Group fields list. / Lista de grupos de campos.'),
              bigquery.SchemaField("unique_key", "INTEGER", description='Total unique register. / Es la cantidad de registros únicos.'),
              bigquery.SchemaField("column_null", "STRING", description='Total empty columns of the table. / Columnas vacías de la tabla.'),
              bigquery.SchemaField("start_time", "TIMESTAMP", description='Profiling run start time. / Es el tiempo de inicio de ejecución del perfilamiento.'),
              bigquery.SchemaField("table_profile_quality", "STRING", description='Destination table. / Nombre de la tabla de destino.'),
              bigquery.SchemaField("run_time", "TIMESTAMP", description='Profiling run end time. / Es el tiempo de final de ejecución del perfilamiento.')]
    try:
        client.get_table(table_id)
        publish(df,client,table_id,schema)
    except NotFound:
        print("tabla {} inexistente, revisar instalación".format(destination_table))     

#-publish_validity
#    Método encargado de crear y publicar en bigquery el dataframe con la dimensión de calidad de validez, proveniente de: set_integer o set_interval o set_interval_year.    
def publish_validity(df,client,dataset_name,destination_table):
    table_id = "{}.{}".format(dataset_name,destination_table)
    schema = [bigquery.SchemaField("table_initial", "STRING", description='Name of the source table. / Nombre de la tabla de origen.'),
              bigquery.SchemaField("date_run", "TIMESTAMP", description='Profiling run datetime. / Corresponde a la fecha y hora de ejecución del perfilamiento.'),
              bigquery.SchemaField("epoch", "INTEGER", description='Value in epoch. / Es el valor de tiempo en epoch.'),
              bigquery.SchemaField("field_review", "STRING", description='Field to validity. / Campos a validar.'),
              bigquery.SchemaField("count_register", "INTEGER", description='Total numbers of records. / Es el numero total de registros.'),
              bigquery.SchemaField("validity", "INTEGER", description='Total number of validity. / Es el numero total de registros validadados.'),
              bigquery.SchemaField("validity_match_per", "FLOAT", description='Porcentage value validity data. / Es el valor porcentual de datos validados.'),
              bigquery.SchemaField("no_validity", "INTEGER", description='Total number of no validity. / Es el numero total de registros no validadados.'),
              bigquery.SchemaField("validity_No_match_per", "FLOAT", description='Porcentage value no validity data. / Es el valor porcentual de datos no validados.')]
    try:
        client.get_table(table_id)
        publish(df,client,table_id,schema)
    except NotFound:
        print("tabla {} inexistente, revisar instalación".format(destination_table))

def get_fields_timestamp(tableref):
    table_schema = tableref.schema
    timestamp_fields = []
    for index, schemafield in enumerate(table_schema):
        if (schemafield.field_type=="TIMESTAMP"):
            timestamp_fields.append(schemafield.name)
    return timestamp_fields

def crear_query(timestamp_fields,array_sql,array_project,array_dataset_name,array_table_name):
    query = "SELECT"
    if(len(timestamp_fields)>0 & array_sql == "*"):
        query = "SELECT * EXCEPT ({}) FROM `{}.{}.{}`".format((', '.join(timestamp_fields)),array_project,array_dataset_name,array_table_name)
    else:
        if(array_sql == "*"):
            query = "SELECT * FROM `{}.{}.{}`".format(array_project,array_dataset_name,array_table_name)    
        else:
            fields_query = array_sql.split(", ")
            for field in timestamp_fields:
                if (field in fields_query):
                    fields_query.remove(field)
                    query = "SELECT {} FROM `{}.{}.{}`".format((', '.join(fields_query)),array_project,array_dataset_name,array_table_name)  
    return query

import os

from flask import Flask , request
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession
from google.cloud import bigquery
import datetime, time


app = Flask(__name__)

#VARIABLES DE ENTORNO
#PORT
#TEMP_BUCKET
#OUTPUT_DATASET
#OUTPUT_TABLE

#Se asume formato 
# #[project,dataset_name,table_name,destination_dataset, destination_table,sql,values,fields,validity_field, type_data,rule_validity,destination_table_validity]

@app.route('/data_quality',methods = ['POST']
def calidad_tabla():
    #      #[project,          dataset_name, table_name,             destination_datasetdestination_table,          sql,                                    values, fields,validity_field, type_data,rule_validity,destination_table_validity]
    mock = "fif-sfa-pe-bi-qa|raw_seg_pe_qa|segpepr_cart_medio_pago|fif_bfa_ccpd|fif_bfa_ccpd_dimension_comparative|CAMD_CD_MEDIO_PAGO,CAMD_DE_MEDIO_PAGO|[NULL,null]|CAMD_CD_MEDIO_PAGO||||fif_bfa_ccpd_validity_comparative"
    lista = mock.split('|')
    project,dataset_name,table_name,destination_dataset,destination_table,sql,values,fields,validity_field,type_data,rule_validity,destination_table_validity = data_row_to_list(lista)
    #Se obtiene el dataframe

    start_process = datetime.datetime.now()
    table_id = "{}.{}.{}".format(project,dataset_name,table_name)

    dataframe = obtener_procesar_dataframe(table_id)

    client = bigquery.Client()
    #Se corren las metricas
    correr_metricas()
  
if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=int(os.environ.get("PORT", 8080)))

def obtener_procesar_dataframe(table_id):
    spark = SparkSession \
      .builder \
      .master('yarn') \
      .appName('data-quality') \
      .getOrCreate()

   # Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
   # bucket = os.environ.get("TEMP_BUCKET", 8080)
    bucket = "[dataproc-temp-us-central1-588221884890-9g3quofy]"
    spark.conf.set('temporaryGcsBucket', bucket)

    # Load data from BigQuery.
    #table_id
    #tabla = spark.read.format('bigquery').option('table', table_id).load
    tabla = spark.read.format('bigquery').option('table', 'data-quality-gcarcamo.Answers.shows_netflix').load
    query_on_df = spark.sql("SELECT * FROM  tabla");
    query_on_df.show()
    query_on_df.printSchema()

    # Saving the data to BigQuery
    # query_on_df.write.format('bigquery').option('table', 'data-quality-gcarcamo.output_dataset.output_table').save()
    #   .option('table', destionation_dataset+"."+destination_table') \     
return query_on_df

def correr_metricas(table_id,client ,query_on_df,lista,values,fields,start_process):
 #Metricas
    try:

        print("Proceso de perfilamiento iniciado para la tabla: {}".format(table_id))
        missing_specific = filter_fast_values(query_on_df,values)
        unique_sub_table, group_field = unique_group(query_on_df,fields)
        df_table, col_null = none_column(query_on_df)
        date_modified, size_table, count_row = extract_last_modified(table_id,client)
        availability = variation_days(date_modified)
        df_control, start_date = split_field(df_table,availability,table_id,missing_specific,start_process)
        if len(df_control)>0:
            publish_metrics_dimensions(df_control,client,destination_dataset,destination_dataset,destination_table)
            print('Perfilamiento publicado para la tabla: {}'.format(table_id))                                    
        else:
            print('Tabla {} vacia, perfilamiento omitido'.format(table_id)) 
        table = table_name
        final_process = datetime.datetime.now()
        table_end = "{}.{}".format(destionation_dataset,destionation_table)
        df_performance = performance_process(table_id,count_row,start_date,availability,size_table,col_null,start_process,table_end,final_process,unique_sub_table, group_field)
        publish_performance(df_performance,client,array_destination_dataset[k],performance_table)
        print('Performance publicado para la tabla: {}'.format(table_id))
        profile_s.append(table_id)
    except Exception as e:
       print('Perfilamiento fallido para la tabla: {}'.format(table_id))
       profile_f.append(table_id)
return 0

def data_row_to_list(list):
    project = list[0]
    dataset_name = list[1]
    table_name = list[2]
    destination_dataset = list[3]
    destination_table = list[4]
    sql = list[5]
    values = list[6]
    fields = list[7]
    validity_field = list[8]
    type_data = list[9]
    rule_validity = list[10]
    destination_table_validity = list[11]
#    name_field = list(df)
#    array_project = list(df[name_field[0]])
#    array_dataset_name = list(df[name_field[1]])
#    array_table_name = list(df[name_field[2]])
#    array_destination_dataset = list(df[name_field[3]])
#    array_destination_table = list(df[name_field[4]])
#    array_sql = list(df[name_field[5]])
#    array_specific_value = list(df[name_field[6]])
#    array_specific_field = list(df[name_field[7]])
#    array_field_validity = list(df[name_field[8]])
#    array_type_data = list(df[name_field[9]])
#    array_condition = list(df[name_field[10]])
#    array_destination_table_validity = list(df[name_field[11]])
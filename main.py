import os

from flask import Flask , request
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession
from google.cloud import bigquery
import json 
import datetime, time


app = Flask(__name__)
#Se asume formato 
# #[project,dataset_name,table_name,destination_dataset, destination_table,sql,values,fields,validity_field, type_data,rule_validity,destination_table_validity]

#Se expone post controller para obtenicon de info de job. Pero al parecer es desde los archivos de input que envie el Cloud run
@app.route('/data_quality',methods = ['POST']
def main():
    
    input_json = request.json
    start_process = datetime.datetime.now()

    project,dataset_name,table_name,destination_dataset,destination_table,sql,values,fields,validity_field,type_data,rule_validity,destination_table_validity = data_row_to_list(input_json)
    table_id = "{}.{}.{}".format(project,dataset_name,table_name)
    
    #Se obtiene el dataframe
    dataframe = obtener_procesar_dataframe(table_id)

    procesar_dataframe(dataframe)

    client = bigquery.Client()
    #Se corren las metricas
    correr_metricas()
  
if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=int(os.environ.get("PORT", 8080)))

def obtener_dataframe(table_id):

    #sc = SparkContext()
    spark = SparkSession.builder.master('yarn').appName('main').getOrCreate()

   # Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
    bucket = os.environ.get("TEMP_BUCKET")
    spark.conf.set('temporaryGcsBucket', bucket)

    # Load data from BigQuery.
    dataframe = spark.read.format('bigquery').option('table', table_id).load

   
    query_on_df = spark.sql("SELECT * FROM  dataframe");
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
       # missing_specific = filter_fast_values(query_on_df,values)
       # unique_sub_table, group_field = unique_group(query_on_df,fields)
       # df_table, col_null = none_column(query_on_df)
       # date_modified, size_table, count_row = extract_last_modified(table_id,client)
       # availability = variation_days(date_modified)
       # df_control, start_date = split_field(df_table,availability,table_id,missing_specific,start_process)
        if len(df_control)>0:
            publish_metrics_dimensions(df_control,client,destination_dataset,destination_dataset,destination_table)
       #     print('Perfilamiento publicado para la tabla: {}'.format(table_id))                                    
       # else:
       #     print('Tabla {} vacia, perfilamiento omitido'.format(table_id)) 
       # table = table_name
       # final_process = datetime.datetime.now()
       # table_end = "{}.{}".format(destionation_dataset,destionation_table)
        df_performance = performance_process(table_id,count_row,start_date,availability,size_table,col_null,start_process,table_end,final_process,unique_sub_table, group_field)
        publish_performance(df_performance,client,destionation_dataset,performance_table)
       # print('Performance publicado para la tabla: {}'.format(table_id))
       # profile_s.append(table_id)
    except Exception as e:
       print('Perfilamiento fallido para la tabla: {}'.format(table_id))
       profile_f.append(table_id)
return

def json_vars(json):
    project = json["project"]
    dataset_name = json["dataset_name"]
    table_name = json["table_name"]
    destination_dataset = json["destination_dataset"]
    destination_table = json["destination_table"]
    sql = json["sql"]
    values = json["values"]
    fields = json["fields"]
    validity_field = json["validity_field"]
    type_data = json["type_data"]
    rule_validity = json["rule_validity"]
    destination_table_validity = json["destination_table_validity"]
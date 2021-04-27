import os

from flask import Flask , request
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession

app = Flask(__name__)

#VARIABLES DE ENTORNO
#PORT
#TEMP_BUCKET
#OUTPUT_DATASET
#OUTPUT_TABLE

#Se asume formato 
# #[project,dataset_name,table_name,destination_dataset,
# destination_table,sql,values,fields,validity_field,
# type_data,rule_validity,destination_table_validity]

@app.route('/data_quality',methods = ['POST']
def calidad_tabla():
    #      #[project,          dataset_name, table_name,             destination_datasetdestination_table,          sql,                                    values, fields,validity_field, type_data,rule_validity,destination_table_validity]
    mock = "fif-sfa-pe-bi-qa|raw_seg_pe_qa|segpepr_cart_medio_pago|fif_bfa_ccpd|fif_bfa_ccpd_dimension_comparative|CAMD_CD_MEDIO_PAGO,CAMD_DE_MEDIO_PAGO|[NULL,null]|CAMD_CD_MEDIO_PAGO||||fif_bfa_ccpd_validity_comparative"
    lista = mock.split('|')

    #Se obtiene el dataframe
    dataframe = obtener_procesar_dataframe(lista)

    #Se corren las metricas
    correr_metricas()
  
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
# [END run_helloworld_service]
# [END cloudrun_helloworld_service]

def obtener_procesar_dataframe(lista):
    spark = SparkSession \
      .builder \
      .master('yarn') \
      .appName('data-quality') \
      .getOrCreate()

    # Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
    bucket = os.environ.get("TEMP_BUCKET", 8080)
    spark.conf.set('temporaryGcsBucket', bucket)

    # Load data from BigQuery.
    bq_table = spark.read.format('bigquery') \
    #.option('table', lista.[]+lista.[]+lista.[])
    .option('table', lista.project+"."+lista.dataset+"."lista.table)
    .load()
    
    word_count.write.format('bigquery') \
    .option('table', os.environment.get('OUTPUT_DATASET')+"."+os.environment.get('OUTPUT_TABLE')) \
    .save()   

def correr_metricas():
 #Metricas    
 #   try:
        #print("Proceso de perfilamiento iniciado para la tabla: {}".format(table_id))
        #missing_specific = filter_fast_values(df_table,array_specific_value[k])
        #unique_sub_table, group_field = unique_group(df_table,array_specific_field[k])
        #df_table, col_null = none_column(df_table)
        #date_modified, size_table, count_row = extract_last_modified(table_id,client)
        #availability = variation_days(date_modified)
        #df_control, start_date = split_field(df_table,availability,table_id,missing_specific,start_process)
        #if len(df_control)>0:
        #    publish_metrics_dimensions(df_control,client,array_destination_dataset[k],array_destination_table[k])
        #    print('Perfilamiento publicado para la tabla: {}'.format(table_id))                                    
        #else:
        #    print('Tabla {} vacia, perfilamiento omitido'.format(table_id)) 
        #table = array_table_name[k]
        #final_process = datetime.datetime.now()
        #table_end = "{}.{}".format(array_destination_dataset[k],array_destination_table[k])
        #df_performance = performance_process(table_id,count_row,start_date,availability,size_table,col_null,start_process,table_end,final_process,unique_sub_table, group_field)
        #publish_performance(df_performance,client,array_destination_dataset[k],performance_table)
        #print('Performance publicado para la tabla: {}'.format(table_id))
        #profile_s.append(table_id)
    #except Exception as e:
     #   print('Perfilamiento fallido para la tabla: {}'.format(table_id))
      #  profile_f.append(table_id)

    print()
return "TODO's migrar perfilamiento"


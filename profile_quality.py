import pandas as pd
import subprocess
import os.path
import json
import datetime, time
from datetime import date
from google.cloud import bigquery

#############
# Librerias #
#############

#Es donde se aloja los métodos principales que realizan el perfilamiento y performance del proceso.
#Utilidad: Librerias de apoyo para el ccpd.


#-none_column
#    Recibe un dataframe e identifica las columnas completamente vacías.
def none_column(df):
    column = list(df)
    number_row = df.shape[0]
    field_null = " "
    for k in range(0,len(column)):
        count_none = df[column[k]].isnull().sum()
        if count_none == number_row:
            del df[column[k]]
            field_null = "{} / {}".format(column[k],field_null)

    return df,str(field_null)


#-extract_last_modified
#    De la metadata de la tabla se extrea valores específicos del .json, en este caso fecha de modificación (valor en epoch), tamaño de la tabla, total de registros (filas).
def extract_last_modified(table_id,client):
    table = client.get_table(table_id)
    date_modified = table.modified
    size_table = table.num_bytes
    count_row = table.num_rows
    return date_modified, str(size_table), count_row


#-variation_days
#    Toma el valor de la fecha en formato "epoch" y retorna la diferencia del día de ejecución del ccpd menos la actualización de la tabla. Obteniendo la dimensión de calidad de "Disponibilidad".
def variation_days(date_modified):
    date_last = date_modified.strftime("%Y-%m-%d")
    edited_date = datetime.datetime.strptime(date_last, "%Y-%m-%d").date()
    run_process = date.today()
    start_date = date(run_process.year,run_process.month,run_process.day)
    availability = (start_date - edited_date).days
    return availability


#-filter_fast_values
#    Se le pasa un dataframe junto un listado de valores específicos para así obtener un total de su frecuencia.
#def filter_fast_values(df,value_list):
#    if len(value_list)==0:
#        value_list = [""]
#    dim =list(df)
#    count_specific = []
#    for i in range(0,len(dim)):
#        field = dim[i]
#        count = 0
#        for k in range(0,len(value_list)):
#            if value_list[k] in list(df[field]):
#                df_filtro =df[df[field].isin([value_list[k]])]
#                n = df_filtro.shape[0]
#                count = n + count
#            else:
#                continue
#        count_specific.append(count)
#    return count_specific

def filter_fast_values(df,value_list):
    if len(value_list)==0:
        value_list = [""]
    dim =list(df)
    count_specific = []
    for i in range(0,len(dim)):
        field = dim[i]
        count = 0
        v_counts = df[field].value_counts()
        for value in value_list:
            if value in v_counts:
                count=+v_counts[value]
        count_specific.append(count)
    return count_specific


#-unique_group
#    Se le pasa un dataframe junto un listado de campos específicos y calcular la unicidad de sus registros.
def unique_group(df,array_field):
    if len(array_field)==0:
        unique_sub_table = 0
    else:
        unique_sub_table = df[array_field].drop_duplicates().shape[0]
    group_field = ''
    for j in range(0,len(array_field)):
        group_field = "{}|{}".format(array_field[j],group_field)
    
    return unique_sub_table, group_field


#-split_field
#    Genera el dataframe con las dimensiones de calidad (completitud, disponibilidad, unicidad), kpi y distribución porcentual por campos de una tabla.
def split_field(df_data_view,availability,table_id,missing_specific,start_process):

    start_date = datetime.datetime.now()
    field = list(df_data_view)
    array_register = df_data_view.shape[0]
    n_field = df_data_view.shape[1]
    df_control = pd.DataFrame(columns=['tables_bq',
                                       'availability',
                                       'epoch',
                                       'date_run',
                                       'name_field',
                                       'count_register',
                                       'count_field',
                                       'completeness_NaN_count',
                                       'completeness_NaN_per',
                                       'completeness_No_NaN_count',
                                       'completeness_No_NaN_per',
                                       'uniqueness_count',
                                       'uniqueness_per',
                                       'value_moda',
                                       'count_moda',
                                       'moda_per'])

    for i in range(0,n_field):
        array_column = field[i]
        df_column = df_data_view[field[i]].copy(deep=False)
        missing_NaN = df_column.isnull().sum()        
        missing_total = missing_specific[i] + missing_NaN
        missing_values = 100*(int(missing_total) / int(array_register))
        no_missing_total = abs(int(array_register) - int(missing_total))
        no_missing_values = 100*( no_missing_total / int(array_register))
        unique_total = df_column.unique().shape[0]
        unique_values = 100*(int(unique_total)/int(array_register))
        moda = df_column.mode()
        frequency_moda = df_column.value_counts()[moda[0]]
        moda_values = 100*(frequency_moda/int(array_register))
        missing_total = int(missing_total)
        array_moda = str(moda.tolist())
        df_control.loc[i] = [table_id,
                            availability,
                            int(time.mktime(start_process.timetuple())),
                            start_process,
                            array_column,
                            array_register,
                            n_field,
                            missing_total,
                            missing_values,
                            no_missing_total,
                            no_missing_values,
                            unique_total,
                            unique_values,
                            array_moda,
                            frequency_moda,
                            moda_values]

        df_control = df_control.round({"completeness_NaN_per":2, "completeness_No_NaN_per":2,"uniqueness_per":2,"moda_per":2}) 

    return df_control, start_date


#-performance_process
#    Genera un dataframe resumen del performance del proceso por tablas.
def performance_process(table_id,count_row,start_date,availability,size_table,col_null,start_process,table_end,final_process,unique_sub_table, gruop_field):
    new_colunms = {'table_initial': [table_id],
                    'count_row': [count_row],
                    'epoch': [int(time.mktime(start_process.timetuple()))],
                    'date_run': [start_process],
                    'availability': [availability],
                    'size_table_KB': [size_table],
                    'key_fields': [gruop_field],
                    'unique_key':[unique_sub_table],
                    'column_null': [col_null],
                    'start_time': [start_date],
                    'table_profile_quality': [table_end],
                    'run_time': [final_process]}

    df_performance = pd.DataFrame(data=new_colunms)
    return df_performance
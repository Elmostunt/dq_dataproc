import pandas as pd
import datetime, time
import json
from datetime import date
import os
from builder_component import *

#############
# Librerias #
#############

#Es donde se ubican las distintas clases y métodos ocupados para el la dimensión de validez del CCPD.
#Utilidad: Librerias de apoyo para el ccpd.

#-type_data_field
#    Genera dos dataframe, el primero es un cruce de campos con sus tipos de datos; el segundo valida que los tipos datos con la metadata de la tabla.

def type_data_field(table_id,field_name,type_data,client):
    table=client.get_table(table_id)
    df_table = pd.DataFrame()
    df_table['schema'] = table.schema
    df_table['field_name'] = df_table.apply(lambda row: getattr(row["schema"],"name"), axis =1)
    df_table['data_type'] = df_table.apply(lambda row: getattr(row["schema"],"field_type"), axis =1)
    df_table['validation_field'] = df_table['field_name'].apply(lambda x: 1 if x == field_name else 0)
    df_table['validation_type_data'] = df_table['data_type'].apply(lambda x: 1 if x == type_data else 0)
    df_table['validation_input'] = df_table['validation_field'] + df_table['validation_type_data']
    df_table['validation_input'] = df_table['validation_input'].apply(lambda x: 'OK' if x == 2 else 'NO_MATCH')
    df_table.drop(['schema','validation_field','validation_type_data'], axis='columns', inplace=True)
    df_match = df_table[df_table['validation_input'].isin(["OK"])]
    return df_table,df_match

#-field_validation
#    Valida que los campos señalados en el input y sus tipos de datos coincidan con la metadata, y retorna un dataframe con los campos Ok.
def field_validation(table_id,array_field_name,array_type_data,client):
    df_match = pd.DataFrame()
    for j in range(0,len(array_field_name)):
        df_table,df_ok = type_data_field(table_id,array_field_name[j],array_type_data[j],client)
        df_match = pd.concat([df_match,df_ok], axis=0)
        
    x = list(df_match)
    match_field = list(df_match[x[0]])
    status_match = list(df_match[x[2]])
    result = dict(zip(match_field, status_match))
    df_table['validation_input'] = df_table['field_name'].apply(lambda x: result.get(x))
    
    return df_table


#-set_integer
#    Retorna un dataframe procesado con la dimensión de validez usando valores numéricos como cota superior o inferior (considerando reglas de validez para INTEGER/FLOAT).
def set_integer(df,table,field,start_process,condition):
    df_view = pd.DataFrame()
    df_view = pd.concat([df_view , df[[field]]], axis=1)
    register = df_view.shape[0]

    if condition =='case_positive_strict':
        df_view.loc[df_view[field] > 0, 'validator'] = True
    elif condition =='case_positive':
        df_view.loc[df_view[field] >= 0, 'validator'] = True
    elif condition =='case_negative_strict':
        df_view.loc[df_view[field] < 0, 'validator'] = True
    elif condition =='case_negative':
        df_view.loc[df_view[field] <= 0, 'validator'] = True
    no_match = df_view['validator'].isnull().sum()
    match = df_view.shape[0] - no_match
    no_match_value = 100*(no_match/register)
    match_value = 100*(match/register)
    new_colunms = {'table_initial': [table], 
                   'date_run':[start_process],
                   'epoch':[int(time.mktime(start_process.timetuple()))],
                   'field_review':[field],
                   'count_register': [register],
                   'validity': [match],
                   'validity_match_per':[match_value],
                   'no_validity': [no_match],
                   'validity_No_match_per':[no_match_value]}
    df_validity = pd.DataFrame(data=new_colunms)
    df_validity = df_validity.round({'validity_match_per':2, 'validity_No_match_per':2})
    return df_validity


#-set_interval
#    Retorna un dataframe procesado con la dimensión de validez usando rangos numéricos como cota superior e inferior y tipos de intervalos: cerrados, abiertos, semi-cerrado ó semi-abiertos (considerando reglas de validez para INTEGER/FLOAT).
def set_interval(df,table,field,start_process,interval):
    df_view = pd.DataFrame()
    df_view = pd.concat([df_view , df[[field]]], axis=1)
    df_view=df_view.reindex(columns=df_view.columns.tolist()+["validator_sup","validator_inf","validator"])
    register = df_view.shape[0]

    if interval[0] == 1:
        df_view.loc[df_view[field] <= interval[2], 'validator_sup'] = True
        df_view.loc[df_view[field] >= interval[1], 'validator_inf'] = True
        df_view.loc[df_view['validator_sup'] == df_view['validator_inf'], 'validator'] = True
    elif interval[0] == 2:
        df_view.loc[df_view[field] < interval[2], 'validator_sup'] = True
        df_view.loc[df_view[field] > interval[1], 'validator_inf'] = True
        df_view.loc[df_view['validator_sup'] == df_view['validator_inf'], 'validator'] = True
    elif interval[0] == 3:
        df_view.loc[df_view[field] < interval[2], 'validator_sup'] = True
        df_view.loc[df_view[field] >= interval[1], 'validator_inf'] = True
        df_view.loc[df_view['validator_sup'] == df_view['validator_inf'], 'validator'] = True
    elif interval[0] == 4:
        df_view.loc[df_view[field] <= interval[2], 'validator_sup'] = True
        df_view.loc[df_view[field] > interval[1], 'validator_inf'] = True
        df_view.loc[df_view['validator_sup'] == df_view['validator_inf'], 'validator'] = True

    no_match = df_view['validator'].isnull().sum()
    match = df_view.shape[0] - no_match
    if register > 0:        
        no_match_value = 100*(no_match/register)
        match_value = 100*(match/register)
    else:
        no_match_value = 0
        match_value = 100
    new_colunms = {'table_initial': [table], 
                   'date_run':[start_process],
                   'epoch':[int(time.mktime(start_process.timetuple()))],
                   'field_review':[field],
                   'count_register': [register],
                   'validity': [match],
                   'validity_match_per':[match_value],
                   'no_validity': [no_match],
                   'validity_No_match_per':[no_match_value]}
    df_validity = pd.DataFrame(data=new_colunms)
    df_validity = df_validity.round({'validity_match_per':2, 'validity_No_match_per':2})

    return df_validity

#-range_time
#    Recibe una variable date del momento de la ejecución del CCPD y otra tipo DATE/TIMESTAMP, y retorna la diferencia en años. 
def range_time(today_date,string):
    from datetime import datetime
    format_date = "%Y-%m-%d"
    val_date = datetime.strptime(string, format_date)
    val_date = date(val_date.year, val_date.month, val_date.day)
    total_days = (today_date - val_date).days
    total_days =  int(total_days / 365)
    return total_days


#-set_interval_year
#    Retorna un dataframe procesado con la dimensión de validez usando valores numéricos como cota superior o inferior (considerando reglas de validez para DATE).
def set_interval_year(df,table,field,start_process,interval):
    run_process = date.today()
    today_date = date(run_process.year,run_process.month,run_process.day)
    df['date_to_number_years'] = df[field].apply(lambda x: str(x.date()))
    df['date_to_number_years'] = df['date_to_number_years'].apply(lambda x: range_time(run_process,x))
    fields = [field,'date_to_number_years']
    df_view = pd.DataFrame()
    df_view = pd.concat([df_view , df[fields]], axis=1)
    register = df_view.shape[0]

    if interval[0] == 1:
        df_view.loc[df_view[fields[1]] <= interval[2], 'validator_sup'] = True
        df_view.loc[df_view[fields[1]] >= interval[1], 'validator_inf'] = True
        df_view.loc[df_view['validator_sup'] == df_view['validator_inf'], 'validator'] = True
    elif interval[0] == 2:
        df_view.loc[df_view[fields[1]] < interval[2], 'validator_sup'] = True
        df_view.loc[df_view[fields[1]] > interval[1], 'validator_inf'] = True
        df_view.loc[df_view['validator_sup'] == df_view['validator_inf'], 'validator'] = True
    elif interval[0] == 3:
        df_view.loc[df_view[fields[1]] < interval[2], 'validator_sup'] = True
        df_view.loc[df_view[fields[1]] >= interval[1], 'validator_inf'] = True
        df_view.loc[df_view['validator_sup'] == df_view['validator_inf'], 'validator'] = True
    elif interval[0] == 4:
        df_view.loc[df_view[fields[1]] <= interval[2], 'validator_sup'] = True
        df_view.loc[df_view[fields[1]] > interval[1], 'validator_inf'] = True
        df_view.loc[df_view['validator_sup'] == df_view['validator_inf'], 'validator'] = True
   
    no_match = df_view['validator'].isnull().sum()
    match = df_view.shape[0] - no_match
    no_match_value = 100*(no_match/register)
    match_value = 100*(match/register)
    new_colunms = {'table_initial': [table], 
                   'date_run':[start_process],
                   'epoch':[int(time.mktime(start_process.timetuple()))],
                   'field_review':[field],
                   'count_register': [register],
                   'validity': [match],
                   'validity_match_per':[match_value],
                   'no_validity': [no_match],
                   'validity_No_match_per':[no_match_value]}
    
    df_validity = pd.DataFrame(data=new_colunms)
    df_validity = df_validity.round({'validity_match_per':2, 'validity_No_match_per':2})
    return df_validity


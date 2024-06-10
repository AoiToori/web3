import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
import numpy as np
from datetime import datetime, timedelta

# Autenticación
SERVICE_ACCOUNT_FILE = 'C:/Users/Andy/Downloads/magnetic-tenure-331920-d606890050d5.json'  # Ruta real a tu archivo de credenciales
FOLDER_ID = '1pt91CLYTYR-f0gedUeMTmVYbFG14O7NE'  # ID del folder en Google Drive

# Autenticación
SCOPES = ['https://www.googleapis.com/auth/drive']

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('drive', 'v3', credentials=credentials)

# Función para buscar el ID de un archivo en un folder específico por nombre
def get_file_id_by_name(folder_id, file_name, service):
    query = f"'{folder_id}' in parents and name='{file_name}'"
    response = service.files().list(q=query, spaces='drive', fields='files(id)').execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']  # Devuelve el ID del primer archivo encontrado con el nombre dado
    else:
        print(f'Archivo "{file_name}" no encontrado en el folder con ID: {folder_id}')
        return None

file_names = ['data_m.txt', 'data_h.txt','estac.txt', 'prop_esta.txt']  # Agrega aquí los nombres de los archivos que deseas leer

# DataFrames para almacenar los datos de los archivos
dfs = []

# Leer y convertir cada archivo en un DataFrame
for file_name in file_names:
    # Obtener el ID del archivo de texto dentro del folder
    file_id = get_file_id_by_name(FOLDER_ID, file_name, service)

    # Si se encontró el archivo, leer su contenido con pandas
    if file_id:
        request = service.files().get_media(fileId=file_id)
        file_content = request.execute()

        # Decodificar el contenido del archivo de texto
        text_content = file_content.decode('utf-8')

        # Crear un DataFrame desde el contenido del archivo de texto
        df = pd.read_csv(io.StringIO(text_content), delimiter=',')  # Aquí puedes especificar el delimitador adecuado

        # Agregar el DataFrame a la lista
        dfs.append(df)

# Ahora tienes dos DataFrames, uno por cada archivo de texto
# Puedes acceder a ellos mediante el índice en la lista dfs
bd_1 = dfs[0]
bd_1_h = dfs[1]

bd_1['C_COD_ESTA'] = bd_1['C_COD_ESTA'].astype(str)
bd_1_h['C_COD_ESTA'] = bd_1_h['C_COD_ESTA'].astype(str)

sec_estar_auto = dfs[2]

# funcion para convertir grados caracteres
def convertir_a_direccion(grados):
    if grados < 0 or grados > 360:
        return "Valor inválido"
    direccion_rangos = [
        (0, 0.01, 'C'),
        (0.01, 11.25, 'N'),
        (11.25, 33.75, 'NNE'),
        (33.75, 56.25, 'NE'),
        (56.25, 78.75, 'ENE'),
        (78.75, 101.25, 'E'),
        (101.25, 123.75, 'ESE'),
        (123.75, 146.25, 'SE'),
        (146.25, 168.75, 'SSE'),
        (168.75, 191.25, 'S'),
        (191.25, 213.75, 'SSW'),
        (213.75, 236.25, 'SW'),
        (236.25, 258.75, 'WSW'),
        (258.75, 281.25, 'W'),
        (281.25, 303.75, 'WNW'),
        (303.75, 326.25, 'NW'),
        (326.25, 348.75, 'NNW'),
        (348.75, 360, 'N')
    ]

    for rango in direccion_rangos:
        inicio, fin, direccion = rango
        if grados >= inicio and grados < fin:
            return direccion
# criterio de horas para hidro
def odl6(hora):
    if pd.isna(hora):
        return 0
    elif hora == '00:00':
        return 3
    elif hora < '06:00' or hora > '07:00' and hora != '00:00' :
        return 2
    elif '06:00' <= hora <= '07:00':
        return 1
    elif hora == '00:00':
        return 3
def odl10(hora):
    if pd.isna(hora):
        return 0
    elif hora == '00:00':
        return 3
    elif hora < '10:00' or hora > '11:00' and hora != '00:00' :
        return 2
    elif '10:00' <= hora <= '11:00':
        return 1
    elif hora == '00:00':
        return 3
def odl14(hora):
    if pd.isna(hora):
        return 0
    elif hora == '00:00':
        return 3
    elif hora < '14:00' or hora > '15:00' and hora != '00:00':
        return 2
    elif '14:00' <= hora <= '15:00':
        return 1
    elif hora == '00:00':
        return 3
def odl18(hora):
    if pd.isna(hora):
        return 0
    elif hora == '00:00':
        return 3
    elif hora < '18:00' or hora > '19:00' and hora != '00:00'  :
        return 2
    elif '18:00' <= hora <= '19:00' :
        return 1
    elif hora == '00:00':
        return 3

# criterio de horas para meteo
def odl7(hora):
    if pd.isna(hora):
        return 0
    elif hora == '00:00':
        return 3
    elif hora < '07:00' or hora > '08:00' :
        return 2
    elif '07:00' <= hora <= '08:00':
        return 1
    elif hora == '00:00':
        return 3
def odl13(hora):
    if pd.isna(hora):
        return 0
    elif hora == '00:00':
        return 3
    elif hora < '13:00' or hora > '14:00':
        return 2
    elif '13:00' <= hora <= '14:00':
        return 1
    elif hora == '00:00':
        return 3
def odl19(hora):
    if pd.isna(hora):
        return 0
    elif hora == '00:00':
        return 3
    elif hora < '19:00' or hora > '20:00' and hora != '00:00' :
        return 2
    elif '19:00' <= hora <= '20:00':
        return 1
    elif hora == '00:00':
        return 3
# funcion para ganerar la tabla meteo
def dts_orc_repo(bd_1):
    sec_estar_auto = dfs[2]
    # Convertir la columna 'FechaHora' al tipo de datos datetime
    bd_1['D_FEC_PLAN'] = pd.to_datetime(bd_1['D_FEC_PLAN'], format='%Y-%m-%d %H:%M:%S')

    # Separar la fecha y la hora en columnas diferentes
    bd_1['Fecha'] = bd_1['D_FEC_PLAN'].dt.strftime('%d/%m/%Y')
    bd_1['Hora_llegada'] = bd_1['D_FEC_PLAN'].dt.strftime('%H:%M')

    bd_1 = bd_1[~bd_1['C_COD_PARAG'].isin(['NI', 'ME', 'NB'])]
    bd_1['Variable'] = bd_1['C_COD_PARAG'].astype(str).str.cat(bd_1['C_COD_CORRP'].astype(str))

    df = bd_1.pivot_table(index=['C_COD_ESTA', 'Fecha', 'Hora_llegada'], columns='Variable', values='N_VAL_PARA')

    #df_txtn = df[['TM102','TM103']].reset_index()
    #df = df.drop(columns=['PT102', 'PT103','TM102','TM103'])

    #VT101 3 5

    ## Tabla de las 7 horas
    df_7= df[['TM103','TM104','TM107','PT102','VT101','VT102','VI101']]
    df_7= df_7.dropna(subset=['TM103','TM104','TM107','PT102','VT101','VT102','VI101'], thresh=1)
    df_7 = df_7.reset_index()
    #df_txtn df_pt
    #df_7 = pd.merge(df_7, df_txtn, on=['C_COD_ESTA','Fecha','Hora_llegada'], how='inner')
    #df_7  = pd.merge(df_7, df_pt, on=['C_COD_ESTA','Fecha','Hora_llegada'], how='inner')

    df_7.rename(columns={'C_COD_ESTA': 'V_COD_ESTA'}, inplace=True)
    df_7 = df_7.replace(-999.0, np.nan)
    df_7['V_COD_ESTA'] = df_7['V_COD_ESTA'].str.lstrip('0')
    df_7 = df_7.merge(sec_estar_auto, on='V_COD_ESTA', how='left')

    df_7 = df_7[['V_COD_DRE','V_COD_ESTA','V_NOM_ESTA','Fecha','Hora_llegada','TM103','TM104',	'TM107'	,'PT102','VT101','VT102','VI101']]
    df_7['ODLL 7'] = df_7['Hora_llegada'].apply(odl7)
    df_7['VT101'] = df_7['VT101'].apply(convertir_a_direccion)

    ## Tabla de las 13 horas
    df_13= df[['TM105','TM108','VT103','VT104','VI102']]
    df_13= df_13.dropna(subset=['TM105','TM108','VT103','VT104','VI102'], thresh=1)
    df_13 = df_13.reset_index()
    #df_13 = pd.merge(df_13, df_txtn, on=['C_COD_ESTA','Fecha','Hora_llegada'], how='inner')
    #df_13  = pd.merge(df_13, df_pt, on=['C_COD_ESTA','Fecha','Hora_llegada'], how='inner')

    df_13.rename(columns={'C_COD_ESTA': 'V_COD_ESTA'}, inplace=True)
    df_13 = df_13.replace(-999.0, np.nan)
    df_13['V_COD_ESTA'] = df_13['V_COD_ESTA'].str.lstrip('0')
    df_13 = df_13.merge(sec_estar_auto, on='V_COD_ESTA', how='left')

    df_13 = df_13[['V_COD_DRE','V_COD_ESTA','V_NOM_ESTA','Fecha','Hora_llegada','TM105'	,'TM108',	'VT103'	,'VT104',	'VI102']]
    df_13['ODLL 13'] = df_13['Hora_llegada'].apply(odl13)
    df_13['VT103'] = df_13['VT103'].apply(convertir_a_direccion)

    ## Tabla de las 19 horas
    df_19= df[['TM102','TM106','TM109','PT103','VT105','VT106','VI103']]
    df_19= df_19.dropna(subset=['TM102','TM106','TM109','PT103','VT105','VT106','VI103'], thresh=1)
    df_19 = df_19.reset_index()
    #df_19 = pd.merge(df_19, df_txtn, on=['C_COD_ESTA','Fecha','Hora_llegada'], how='inner')
    #df_19  = pd.merge(df_19, df_pt, on=['C_COD_ESTA','Fecha','Hora_llegada'], how='inner')

    df_19.rename(columns={'C_COD_ESTA': 'V_COD_ESTA'}, inplace=True)
    df_19 = df_19.replace(-999.0, np.nan)
    df_19['V_COD_ESTA'] = df_19['V_COD_ESTA'].str.lstrip('0')
    df_19 = df_19.merge(sec_estar_auto, on='V_COD_ESTA', how='left')

    df_19 = df_19[['V_COD_DRE','V_COD_ESTA','V_NOM_ESTA','Fecha','Hora_llegada','TM102','TM106'	,'TM109','PT103','VT105',	'VT106'	,'VI103']]
    df_19['ODLL 19'] = df_19['Hora_llegada'].apply(odl19)
    df_19['VT105'] = df_19['VT105'].apply(convertir_a_direccion)
    return df_7, df_13, df_19

##df_7,df_13,df_19,df_pt
########################

# esitlo colores meteo
def apply_styles_to_cells(df):
    conditional_formatting = []
    imp =  dfs[3]
    imp['VCODESTA'] = imp['VCODESTA'].astype(str)
    for index, row in df.iterrows():
        for h in range(len(np.array(imp['VCODESTA']))):
            if row['Código'] == imp['VCODESTA'][h] :
                conditional_formatting.append( {'if': {'row_index': index, 'column_id': ['Código']}, 'backgroundColor': '#CD9E3A'})

        if row['Op07'] == 1:
            conditional_formatting.append(
                {'if': {'row_index': index, 'column_id': ['Op07','Hora07']}, 'backgroundColor': '#B9E9CF'})
        elif row['Op07'] == 2:
            conditional_formatting.append(
                {'if': {'row_index': index, 'column_id':  ['Op07','Hora07']}, 'backgroundColor': '#FFDB69'})
        elif row['Op07'] == 0:
            conditional_formatting.append({'if': {'row_index': index, 'column_id':  ['Op07','Hora07']}, 'backgroundColor': '#FF8989'})
        elif row['Op07'] == 3:
            conditional_formatting.append({'if': {'row_index': index, 'column_id':  ['Op07','Hora07']}, 'backgroundColor': '#9B9BFF'})


        if row['Op13'] == 1:
            conditional_formatting.append(
                {'if': {'row_index': index, 'column_id': ['Op13','Hora13']}, 'backgroundColor': '#B9E9CF'})
        elif row['Op13'] == 2:
            conditional_formatting.append(
                {'if': {'row_index': index, 'column_id': ['Op13','Hora13']}, 'backgroundColor': '#FFDB69'})
        elif row['Op13'] == 0:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['Op13','Hora13']}, 'backgroundColor': '#FF8989'})
        elif row['Op13'] == 3:
            conditional_formatting.append(
                {'if': {'row_index': index, 'column_id': ['Op13','Hora13']}, 'backgroundColor': '#9B9BFF'})

        if row['Op19'] == 1:
            conditional_formatting.append(
                {'if': {'row_index': index, 'column_id': ['Op19','Hora19']}, 'backgroundColor': '#B9E9CF'})
        elif row['Op19'] == 2:
            conditional_formatting.append(
                {'if': {'row_index': index, 'column_id':['Op19','Hora19']}, 'backgroundColor': '#FFDB69'})
        elif row['Op19'] == 0:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['Op19','Hora19']}, 'backgroundColor': '#FF8989'})
        elif row['Op19'] == 3:
            conditional_formatting.append(
                {'if': {'row_index': index, 'column_id': ['Op19','Hora19']}, 'backgroundColor': '#9B9BFF'})

###############Igualdades
        if row['TS19'] == row['TMAX'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TMAX','TS19']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})
        if  row['TS13'] == row['TMAX']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TMAX','TS13']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})
        if row['TS7'] == row['TMAX']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TMAX','TS7']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})

        if row['TH7'] == row['TS7']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TH7','TS7']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})
        if row['TH13'] == row['TS13']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TH13','TS13']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})
        if row['TH19'] == row['TS19']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TH19','TS19']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})

        if row['TS7'] == row['TMIN'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TMIN','TS7']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})

        if row['PP7'] == row['TMIN'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['PP7','TMIN']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})
        if row['PP7'] == row['TS7'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['PP7','TS7']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})
        if row['PP7'] == row['TH7'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['PP7','TH7']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})

        if row['PP19'] == row['TMAX'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['PP19','TMAX']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})
        if row['PP19'] == row['TS19'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['PP19','TS19']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})
        if row['PP19'] == row['TH19'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['PP19','TH19']}, 'color': '#000000', 'backgroundColor': '#68F6FF'})

        ############### Diferencias
        if row['TS19'] > row['TMAX'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TMAX','TS19']}, 'color': '#000000', 'backgroundColor': '#FF5757'})
        if  row['TS13'] > row['TMAX']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TMAX','TS13']}, 'color': '#000000', 'backgroundColor': '#FF5757'})
        if row['TS7'] > row['TMAX']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TMAX','TS7']}, 'color': '#000000', 'backgroundColor': '#FF5757'})

        if row['TH7'] > row['TS7']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TH7','TS7']}, 'color': '#000000', 'backgroundColor': '#FF5757'})
        if row['TH13'] > row['TS13']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TH13','TS13']}, 'color': '#000000', 'backgroundColor': '#FF5757'})
        if row['TH19'] > row['TS19']:
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TH19','TS19']}, 'color': '#000000', 'backgroundColor': '#FF5757'})

        if row['TS7'] < row['TMIN'] :
            conditional_formatting.append({'if': {'row_index': index, 'column_id': ['TMIN','TS7']}, 'color': '#000000', 'backgroundColor': '#FF5757'})
    return conditional_formatting

######################################################################################## HIDRO#  funcion para ganerar la tabla hidro
def dts_orc_repo_h(bd_1_h):
    bd_1_h['D_FEC_PLAN'] = pd.to_datetime(bd_1_h['D_FEC_PLAN'], format='%Y-%m-%d %H:%M:%S')

    bd_1_h['Fecha'] = bd_1_h['D_FEC_PLAN'].dt.strftime('%d/%m/%Y')
    bd_1_h['Hora_llegada'] = bd_1_h['D_FEC_PLAN'].dt.strftime('%H:%M')
    bd_1_h['Variable'] = bd_1_h['C_COD_PARAG'].astype(str).str.cat(bd_1_h['C_COD_CORRP'].astype(str))
    bd_1_h = bd_1_h[['C_COD_ESTA', 'Fecha', 'Hora_llegada', 'N_VAL_PARA', 'Variable']]
    df_h = bd_1_h.pivot_table(index=['C_COD_ESTA', 'Fecha', 'Hora_llegada'], columns='Variable', values='N_VAL_PARA')
    df_h = df_h.reset_index()
    df_h.rename(columns={'C_COD_ESTA': 'V_COD_ESTA'}, inplace=True)
    df_h['V_COD_ESTA'] = df_h['V_COD_ESTA'].str.lstrip('0')

    df_6 = df_h[['V_COD_ESTA', 'Fecha', 'Hora_llegada', 'NI101']]
    df_10 = df_h[['V_COD_ESTA', 'Fecha', 'Hora_llegada', 'NI102']]
    df_14 = df_h[['V_COD_ESTA', 'Fecha', 'Hora_llegada', 'NI103']]
    df_18 = df_h[['V_COD_ESTA', 'Fecha', 'Hora_llegada', 'NI104']]

    df_6 = df_6.merge(sec_estar_auto, on='V_COD_ESTA', how='left')
    df_6 = df_6[['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA', 'Hora_llegada', 'Fecha', 'NI101']].dropna(
        subset='NI101')

    df_10 = df_10.merge(sec_estar_auto, on='V_COD_ESTA', how='left')
    df_10 = df_10[['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA', 'Hora_llegada', 'Fecha', 'NI102']].dropna(
        subset='NI102')

    df_14 = df_14.merge(sec_estar_auto, on='V_COD_ESTA', how='left')
    df_14 = df_14[['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA', 'Hora_llegada', 'Fecha', 'NI103']].dropna(
        subset='NI103')

    df_18 = df_18.merge(sec_estar_auto, on='V_COD_ESTA', how='left')
    df_18 = df_18[['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA', 'Hora_llegada', 'Fecha', 'NI104']].dropna(
        subset='NI104')

    return df_6, df_10, df_14,df_18

##df_7,df_13,df_19,df_pt
########################
# estilo colores hidro
def apply_styles_to_cells_h(df):
    conditional_formatting_h = []
    for index, row in df.iterrows():
        if row['Op06'] == 1:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id': ['Op06','Hora06']}, 'backgroundColor': '#B9E9CF'})
        elif row['Op06'] == 2:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id':  ['Op06','Hora06']}, 'backgroundColor': '#FFDB69'})
        elif row['Op06'] == 0:
            conditional_formatting_h.append({'if': {'row_index': index, 'column_id':  ['Op06','Hora06']}, 'backgroundColor': '#FF8989'})
        elif row['Op06'] == 3:
            conditional_formatting_h.append({'if': {'row_index': index, 'column_id':  ['Op06','Hora06']}, 'backgroundColor': '#9B9BFF'})

        if row['Op10'] == 1:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id': ['Op10','Hora10']}, 'backgroundColor': '#B9E9CF'})
        elif row['Op10'] == 2:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id': ['Op10','Hora10']}, 'backgroundColor': '#FFDB69'})
        elif row['Op10'] == 0:
            conditional_formatting_h.append({'if': {'row_index': index, 'column_id': ['Op10','Hora10']}, 'backgroundColor': '#FF8989'})
        elif row['Op10'] == 3:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id': ['Op10','Hora10']}, 'backgroundColor': '#9B9BFF'})

        if row['Op14'] == 1:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id': ['Op14','Hora14']}, 'backgroundColor': '#B9E9CF'})
        elif row['Op14'] == 2:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id':['Op14','Hora14']}, 'backgroundColor': '#FFDB69'})
        elif row['Op14'] == 0:
            conditional_formatting_h.append({'if': {'row_index': index, 'column_id': ['Op14','Hora14']}, 'backgroundColor': '#FF8989'})
        elif row['Op14'] == 3:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id': ['Op14','Hora14']}, 'backgroundColor': '#9B9BFF'})


        if row['Op18'] == 1:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id': ['Op18','Hora18']}, 'backgroundColor': '#B9E9CF'})
        elif row['Op18'] == 2:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id':['Op18','Hora18']}, 'backgroundColor': '#FFDB69'})
        elif row['Op18'] == 0:
            conditional_formatting_h.append({'if': {'row_index': index, 'column_id': ['Op18','Hora18']}, 'backgroundColor': '#FF8989'})
        elif row['Op18'] == 3:
            conditional_formatting_h.append(
                {'if': {'row_index': index, 'column_id': ['Op18','Hora18']}, 'backgroundColor': '#9B9BFF'})

        if row['Nivel06'] >= 19:
            conditional_formatting_h.append({'if': {'row_index': index, 'column_id': ['Nivel06']}, 'color': '#000000', 'backgroundColor': '#FF5757'})
        if row['Nivel10'] >= 19:
            conditional_formatting_h.append({'if': {'row_index': index, 'column_id': ['Nivel10']}, 'color': '#000000', 'backgroundColor': '#FF5757'})
        if row['Nivel14'] >= 19:
            conditional_formatting_h.append({'if': {'row_index': index, 'column_id': ['Nivel14']}, 'color': '#000000', 'backgroundColor': '#FF5757'})
        if row['Nivel18'] >= 19:
            conditional_formatting_h.append({'if': {'row_index': index, 'column_id': ['Nivel18']}, 'color': '#000000', 'backgroundColor': '#FF5757'})

    return conditional_formatting_h


## tabla dash
import dash
from dash import html, dcc, callback, Input, Output
from dash.dependencies import Input, Output
from dash import dash_table
import dash_bootstrap_components as dbc

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    html.H1("Reporte de oportunidad y llegada de datos del SISVyD", style={'text-align': 'center', 'color': '#0215FF'}),

    dbc.Row([
        dbc.Col(
            dcc.DatePickerSingle(
                id='fecha-picker',
                date=datetime.now().date(),
                display_format='DD/MM/YYYY',
            ),
            align="right"
        )
    ]),

    dcc.Tabs(id='tabs', value='tab-1', children=[
        dcc.Tab(label='Meteorología', value='tab-1'),
        dcc.Tab(label='Hidrología', value='tab-2'),
    ]),

    html.Div(id='tab-content')
])
def set_default_date():
    return datetime.now().date()

@app.callback(
    Output('fecha-picker', 'date'),
    Input('tabs', 'value')
)
def update_default_date(selected_tab):
    if selected_tab:
        return set_default_date()
    return dash.no_update

@app.callback(
    Output('tab-content', 'children'),
    [Input('tabs', 'value'),
     Input('fecha-picker', 'date')]
)

def update_table(tab, fecha):
    if tab == 'tab-1':

        df_7, df_13, df_19 = dts_orc_repo(bd_1)

        fecha_seleccionada = pd.to_datetime(fecha).strftime('%d/%m/%Y')
        df_71 = df_7[df_7['Fecha'] == fecha_seleccionada]
        df_131 = df_13[df_13['Fecha'] == fecha_seleccionada]
        df_191 = df_19[df_19['Fecha'] == fecha_seleccionada]

        #df_concat_t = df_pt1.merge(df_71, on=['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA'], how='left')

        df_concat_t = df_71.merge(df_131, on=['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA'], how='outer',
                                        suffixes=('_df7', '_df13'))
        df_concat_t = df_concat_t.merge(df_191, on=['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA'], how='outer',
                                        suffixes=('', '_df19'))
        tabla_df = df_concat_t[
            ['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA', 'Fecha_df7', 'Fecha_df13', 'Fecha', 'ODLL 7',
             'Hora_llegada_df7', 'TM103', 'TM104', 'TM107', 'PT102', 'VT101', 'VT102', 'VI101', 'ODLL 13', 'Hora_llegada_df13',
             'TM105', 'TM108', 'VT103', 'VT104', 'VI102', 'ODLL 19', 'Hora_llegada', 'TM102', 'TM106', 'TM109',
             'PT103',         'VT105', 'VT106', 'VI103']]

        tabla_df['Hora_llegada_df7'] = tabla_df.apply(lambda row: '00:00' if pd.notna(row['PT102']) and pd.isna(row['Hora_llegada_df7']) else row['Hora_llegada_df7'], axis=1)
        tabla_df['Hora_llegada'] = tabla_df.apply(
            lambda row: '00:00' if pd.notna(row['PT103']) and pd.isna(row['Hora_llegada']) else row[
                'Hora_llegada'], axis=1)

        tabla_df['Hora_llegada_df7'] = tabla_df.apply(
            lambda row: '00:00' if pd.notna(row['TM103']) and pd.isna(row['Hora_llegada_df7']) else row['Hora_llegada_df7'],
            axis=1)
        tabla_df['Hora_llegada'] = tabla_df.apply(
            lambda row: '00:00' if pd.notna(row['TM102']) and pd.isna(row['Hora_llegada']) else row[
                'Hora_llegada'], axis=1)

        tabla_df['ODLL 7'] = tabla_df['Hora_llegada_df7'].apply(odl7)
        tabla_df['ODLL 13'] = tabla_df['Hora_llegada_df13'].apply(odl13)
        tabla_df['ODLL 19'] = tabla_df['Hora_llegada'].apply(odl19)

        tabla_df = tabla_df.drop(columns=['Fecha_df7', 'Fecha_df13', 'Fecha'])
        tabla_df.columns = ['DZ', 'Código', 'Estación', 'ODLL7', 'Hora7', 'MIN', 'TS7', 'TH7', 'PP7', 'DIR7', 'VEL7',
                            'VIS7', 'ODLL13', 'Hora13', 'TS13', 'TH13', 'DIR13', 'VEL13', 'VIS13', 'ODLL19', 'Hora19',
                            'MAX', 'TS19', 'TH19', 'PP19', 'DIR19', 'VEL19', 'VIS19']
        tabla_df = tabla_df.sort_values(by=['DZ', 'Código', 'Estación'], ascending=[True, True, True])
        tabla_df.replace(-999, np.nan, inplace=True)
        tabla_df = tabla_df.reset_index()
        tabla_df = tabla_df.drop(['index'], axis=1)
        nuevos_nombres = {
            'MIN': 'TMIN',
            'MAX': 'TMAX',
            'ODLL7': 'Op07',
            'ODLL13': 'Op13',
            'ODLL19': 'Op19',
            'Hora7': 'Hora07'
        }
        ## tabla datos met
        tabla_df = tabla_df.rename(columns=nuevos_nombres)
        imp = dfs[3]
        imp['VCODESTA'] = imp['VCODESTA'].astype(str)
        tabla_df['IMP'] = np.nan
        tabla_df.loc[tabla_df['Código'].isin(imp['VCODESTA']), 'IMP'] = 'BCR'

        conditional_formatting_rows = apply_styles_to_cells(tabla_df)

        return html.Div([
            dash_table.DataTable(
                id='tabla-datos',
                columns=[{'name': col, 'id': col} for col in tabla_df.columns],
                data=tabla_df.to_dict('records'),
                style_table={'overflowX': 'scroll', 'height': '80vh'},
                # Ajusta la altura al 100% de la ventana del navegador
                page_action='none',
                style_cell={'textAlign': 'left'},
                style_data_conditional=conditional_formatting_rows,
                fixed_rows={'headers': True, 'data': 0},
                filter_action="native",
                sort_action="native",
                sort_mode="multi"
            )
        ], style={'height': '100vh'})

    elif tab == 'tab-2':
        df_6, df_10, df_14, df_18 = dts_orc_repo_h(bd_1_h)

        fecha_seleccionada = pd.to_datetime(fecha).strftime('%d/%m/%Y')
        df_61 = df_6[df_6['Fecha'] == fecha_seleccionada]
        df_101 = df_10[df_10['Fecha'] == fecha_seleccionada]
        df_141 = df_14[df_14['Fecha'] == fecha_seleccionada]
        df_181 = df_18[df_18['Fecha'] == fecha_seleccionada]
        # df_concat_t = df_pt1.merge(df_71, on=['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA'], how='left')

        df_concat_t = df_61.merge(df_101, on=['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA'], how='outer',
                                  suffixes=('_df6', '_df10'))
        df_concat_t = df_concat_t.merge(df_141, on=['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA'], how='outer',
                                        suffixes=('', '_df14'))
        df_concat_t = df_concat_t.merge(df_181, on=['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA'], how='outer',
                                        suffixes=('', '_df18'))
        df_concat_t['Op06'] = df_concat_t['Hora_llegada_df6'].apply(odl6)
        df_concat_t['Op10'] = df_concat_t['Hora_llegada_df10'].apply(odl10)
        df_concat_t['Op14'] = df_concat_t['Hora_llegada'].apply(odl14)
        df_concat_t['Op18'] = df_concat_t['Hora_llegada_df18'].apply(odl18)

        tabla_df = df_concat_t[
            ['V_COD_DRE', 'V_COD_ESTA', 'V_NOM_ESTA', 'Op06', 'Hora_llegada_df6', 'NI101', 'Op10', 'Hora_llegada_df10',
             'NI102', 'Op14', 'Hora_llegada', 'NI103', 'Op18', 'Hora_llegada_df18', 'NI104']]
        tabla_df.columns = ['DZ', 'Código', 'Estación', 'Op06', 'Hora06', 'Nivel06', 'Op10', 'Hora10', 'Nivel10',
                            'Op14', 'Hora14', 'Nivel14', 'Op18', 'Hora18', 'Nivel18']
        tabla_df = tabla_df.sort_values(by=['DZ', 'Código', 'Estación'], ascending=[True, True, True])
        tabla_df = tabla_df.reset_index()
        ## tabla datos hidro
        tabla_df = tabla_df.drop(['index'], axis=1)
        conditional_formatting_rows = apply_styles_to_cells_h(tabla_df)

        return html.Div([
            dash_table.DataTable(
                id='tabla-datos',
                columns=[{'name': col, 'id': col} for col in tabla_df.columns],
                data=tabla_df.to_dict('records'),
                style_table={'overflowX': 'scroll', 'height': '120vh'},
                page_action='none',
                style_cell={'textAlign': 'left'},  # Alinear datos a la izquierda
                style_data_conditional=conditional_formatting_rows,
                fixed_rows={'headers': True, 'data': 0},
                filter_action="native",
                sort_action="native",
                sort_mode="multi"
            )
        ], style={'height': '100vh'})

if __name__ == '__main__':
    app.run_server(debug=True)
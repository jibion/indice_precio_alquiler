## Import dependencies
import os
import requests
import pandas as pd
import numpy as np
import logging
from sqlalchemy import *
import pandas as pd
import janitor
import difflib

## Configuration
# Defining our connection variables
username = 'user'  # replace with your username
password = 'pwd'  # replace with your password
ipaddress = 'localhost'  # change this to your db’s IP address
port = 3306  # this is the standard port for MySQL, but change it to your port if needed
dbname = 'database'  # change this to the name of your db

# A long string that contains the necessary MySQL login information
mysql_str = f'mysql+mysqlconnector://{username}:{password}@{ipaddress}:{port}/{dbname}'

# Create the connection
cnx = create_engine(mysql_str)

metadata = MetaData()  # stores the 'production' database's metadata

inspector = inspect(cnx)  # creates a inspector element used to check if we have the tables already

## Define Table Schemas
comunidades = Table('comunidades', metadata,
                    Column('index', Integer, primary_key=True, nullable=False),
                    Column('cca', String(2)),
                    Column('litca', String(55)),
                    schema=dbname)

provincias = Table('provincias', metadata,
                   Column('index', Integer, primary_key=True, nullable=False),
                   Column('cpro', String(3), nullable=False),
                   Column('abr', String(3), nullable=False),
                   Column('litpro', String(55), nullable=False),
                   Column('cca', String(2), ForeignKey("comunidades.cca")),
                   schema='indice_alquiler')

municipios = Table('municipios', metadata,
                   Column('index', Integer, primary_key=True, nullable=False),
                   Column('cumun', String(10), nullable=False),
                   Column('litmun', String(100), nullable=False),
                   Column('cpro', String(3), ForeignKey("provincias.cpro")),
                   schema=dbname)

dataset_provincias = Table('data_provincias', metadata,
                           Column('index', Integer, primary_key=True, nullable=False),
                           Column('cpro', String(10), ForeignKey("secciones.cusec")),
                           Column('type', String(2), nullable=False),
                           Column('year', Integer, nullable=False),
                           Column('n_obs', Float),
                           Column('rent_sqm_median', Float),
                           Column('rent_sqm_p25', Float),
                           Column('rent_sqm_p75', Float),
                           Column('rent_total_median', Float),
                           Column('rent_total_p25', Float),
                           Column('rent_total_p75', Float),
                           Column('size_sqm_median', Float),
                           Column('size_sqm_p25', Float),
                           Column('size_sqm_p75', Float),
                           schema=dbname)

dataset_municipios = Table('data_municipios', metadata,
                           Column('index', Integer, primary_key=True, nullable=False),
                           Column('cumun', String(10), ForeignKey("secciones.cusec")),
                           Column('type', String(2), nullable=False),
                           Column('year', Integer, nullable=False),
                           Column('n_obs', Float),
                           Column('rent_sqm_median', Float),
                           Column('rent_sqm_p25', Float),
                           Column('rent_sqm_p75', Float),
                           Column('rent_total_median', Float),
                           Column('rent_total_p25', Float),
                           Column('rent_total_p75', Float),
                           Column('size_sqm_median', Float),
                           Column('size_sqm_p25', Float),
                           Column('size_sqm_p75', Float),
                           schema=dbname)

## Tables Creation

### Download the file with the Index of Home Rent
# The (xlsx) file contains all data related with the Indice de Alquiler de Vivienda (Index of Home Rent).
# For more information check this page (only available in Spanish):
# [Índice alquiler de vivienda](https://www.mitma.gob.es/vivienda/alquiler/indice-alquiler)

# check whether the path with the source data exists or not
source_data_path = './source_data'
sourceDataPathExist = os.path.exists(source_data_path)

# create a new directory if it does not exist
try:
    os.makedirs(source_data_path)
    print(source_data_path, 'folder created.', sep=' ')
except:
    print(source_data_path, 'already exists. Skipping creation.', sep=' ')

# check whether the file with the rent index exists or not
source_data_file = 'bd_sistema-indices-alquiler-vivienda_2015-2020.xlsx'
full_source_data = os.path.join(source_data_path, source_data_file)
sourceDataFileExist = os.path.exists(full_source_data)

# download the file if it does not exist
file_url = 'https://www.mitma.gob.es/recursos_mfom/comodin/recursos/bd_sistema-indices-alquiler-vivienda_2015-2020.xlsx'
try:
    open(full_source_data)
    print(full_source_data, 'already exists. Skipping download.', sep=' ')
except:
    # fetch file
    r = requests.get(file_url, stream=True)
    if r.ok:
        print("Saving to", os.path.abspath(full_source_data))
        with open(full_source_data, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print("Download failed - status code:", r.status_code, sep=' ')

### Read the file with the Index Home Rent source data (by Municipalities and Provinces)

# threat all as strings
df_municipios = pd.read_excel(full_source_data, sheet_name=2, converters={'CPRO': str, 'CUMUN': str},
                              na_values=['(NA)'], decimal=',').fillna(0)

df_provincias = pd.read_excel(full_source_data, sheet_name=3, converters={'CPRO': str},
                              na_values=['(NA)'], decimal=',').fillna(0)

### Add shortnames for provinces

# extract all of the HTML tables from Wikipedia and do some transformations (clean dataframe)
tablesInPage = pd.read_html("https://es.wikipedia.org/wiki/ISO_3166-2:ES")
prov_shortnames = tablesInPage[1].rename(columns={"Código": "abr", "Nombre de la subdivisión en la ISO[1]​": "name",
                                                  "Comunidad autónoma": "cca"})
prov_shortnames.abr.str.extract('([\\[]]+)', expand=False)
prov_shortnames['abr'] = prov_shortnames['abr'].str.replace(r"\[.*\]", "", regex=True)
prov_shortnames['abr'] = prov_shortnames['abr'].str.strip()
prov_shortnames['abr'] = prov_shortnames['abr'].str.split('-').str[1]
prov_shortnames['name'] = prov_shortnames['name'].str.replace(r"\[.*\]", "", regex=True)
prov_shortnames['name'] = prov_shortnames['name'].str.strip()

# add ceuta and melilla
prov_shortnames.loc[len(prov_shortnames.index)] = ['CE', 'Ceuta', 'CE']
prov_shortnames.loc[len(prov_shortnames.index)] = ['ML', 'Melilla', 'ML']

# do a fuzzy search of provinces
prov_shortnames['LITPRO'] = prov_shortnames['name'].apply(
    lambda x: difflib.get_close_matches(x, df_provincias['LITPRO'], cutoff=0.3)[0])

# merge to provinces dataframe and do some transformations
df_provincias = pd.merge(df_provincias, prov_shortnames[['abr', 'name', 'LITPRO']], on="LITPRO", how="left")
df_provincias['LITPRO'] = df_provincias['name']
df_provincias.drop('name', inplace=True, axis=1)

# extract all of the HTML tables from the INE page (National Staticstics Institue)
tablesInPage = pd.read_html("https://www.ine.es/daco/daco42/codmun/cod_ccaa_provincia.htm",
                            converters={
                                'CODAUTO': str,
                                'CPRO': str,
                            })
ccaa = tablesInPage[0].rename(columns={"CODAUTO": "CCA", "Comunidad Autónoma": "LITCA"})

df_municipios = pd.merge(df_municipios, ccaa[['CCA', 'CPRO']], on="CPRO", how="left")
df_provincias = pd.merge(df_provincias, ccaa[['CCA', 'CPRO']], on="CPRO", how="left")

### Add Comunidad Autonoma (region) to Dataframes


### Table: Comunidades Autonomas (Regions)

ccaa_grouped_df = ccaa.drop_duplicates(['CCA', 'LITCA'])[['CCA', 'LITCA']].reset_index(drop=True)
ccaa_grouped_df = ccaa_grouped_df[ccaa_grouped_df['CCA'].str.len() < 3]

# #### Create the table if does not exists already

# use the inspector to check if the comunidades table exists already
# give the user the chance to drop it and recreate in case it is there
if (inspector.has_table(comunidades.name)):
    print("Table `comunidades` already exists. Type (drop) to drop and recreate, otherwise it will be kept")
    dropSelection = input()
    if dropSelection == "drop":
        comunidades.drop(cnx)
        ccaa_grouped_df.to_sql('comunidades', cnx)
        print("Table `comunidades` recreated")
# try to create it if table is not there
else:
    ccaa_grouped_df.to_sql('comunidades', cnx)
    print("Table `comunidades` created")

### Table: Provinces

provinces_grouped_df = df_provincias.drop_duplicates(
    ['CPRO', 'LITPRO', 'CCA', 'abr'])[['CPRO', 'LITPRO', 'CCA', 'abr']].reset_index(drop=True)

#### Create the table if does not exists already

# use the inspector to check if the provincias table exists already
# give the user the chance to drop it and recreate in case it is there
if (inspector.has_table(provincias.name)):
    print("Table `provincias` already exists. Type (drop) to drop and recreate, otherwise it will be kept")
    dropSelection = input()
    if dropSelection == "drop":
        provincias.drop(cnx)
        provinces_grouped_df.to_sql('provincias', cnx)
        print("Table `provincias` recreated")
# try to create it if table is not there
else:
    provinces_grouped_df.to_sql('provincias', cnx)
    print("Table `provincias` created")

### Table: Municipalities

municipalities_grouped_df = df_municipios.drop_duplicates(['CUMUN', 'LITMUN', 'CPRO'])[
    ['CUMUN', 'LITMUN', 'CPRO']].reset_index(drop=True)

#### Create the table if does not exists already

# use the inspector to check if the municipios table exists already
# give the user the chance to drop it and recreate in case it is there
if (inspector.has_table(municipios.name)):
    print("Table `municipios` already exists. Type (drop) to drop and recreate, otherwise it will be kept")
    dropSelection = input()
    if dropSelection == "drop":
        municipios.drop(cnx)
        municipalities_grouped_df.to_sql('municipios', cnx)
        print("Table `municipios` recreated")
# try to create it if table is not there
else:
    municipalities_grouped_df.to_sql('municipios', cnx)
    print("Table `municipios` created")

### Table: Data (Provinces)

# keep only needed data
data_provincias = df_provincias.drop(['LITPRO', 'CCA', 'abr'], axis=1)
data_provincias

# pivot data as needed
data_provincias_pivot = (data_provincias
                         .pivot_longer(
    index=['CPRO'],
    names_to=('.value', 'type', 'year'),
    names_pattern=r"(\S+)(\S{2})_(\d+)",
    names_transform={'year': int})
                         .assign(year=lambda df: df.year + 2000)
                         )
data_provincias_pivot

data_provincias_pivot.columns = ['cpro', 'type', 'year', 'n_obs', 'rent_sqm_median', 'rent_sqm_p25', 'rent_sqm_p75',
                                 'rent_total_median', 'rent_total_p25', 'rent_total_p75',
                                 'size_sqm_median', 'size_sqm_p25', 'size_sqm_p75']

# keep only rows where there is data in all rows and there is info for price
data_provincias_pivot = data_provincias_pivot.dropna()
data_provincias_pivot = data_provincias_pivot[data_provincias_pivot.rent_sqm_median > 0]

#### Create the table if does not exists already

# use the inspector to check if the secciones table exists already
# give the user the chance to drop it and recreate in case it is there
if (inspector.has_table(dataset_provincias.name)):
    print("Table `data_provincias` already exists. Type (drop) to drop and recreate, otherwise it will be kept")
    dropSelection = input()
    if dropSelection == "drop":
        dataset_provincias.drop(cnx)
        data_provincias_pivot.to_sql('data_provincias', cnx)
        print("Table `data_provincias` recreated")
# try to create it if table is not there
else:
    data_provincias_pivot.to_sql('data_provincias', cnx)
    print("Table `data_provincias` created")

### Table: Data (Municipalities)

# keep only needed data
data_municipios = df_municipios.drop(['CPRO', 'LITPRO', 'LITMUN', 'CCA'], axis=1)

# pivot data as needed
data_municipios_pivot = (data_municipios
                         .pivot_longer(
    index=['CUMUN'],
    names_to=('.value', 'type', 'year'),
    names_pattern=r"(\S+)(\S{2})_(\d+)",
    names_transform={'year': int})
                         .assign(year=lambda df: df.year + 2000)
                         )

data_municipios_pivot.columns = ['cumun', 'type', 'year', 'n_obs', 'rent_sqm_median', 'rent_sqm_p25', 'rent_sqm_p75',
                                 'rent_total_median', 'rent_total_p25', 'rent_total_p75',
                                 'size_sqm_median', 'size_sqm_p25', 'size_sqm_p75']

# keep only rows where there is data in all rows and there is info for price
data_municipios_pivot = data_municipios_pivot.dropna()
data_municipios_pivot = data_municipios_pivot[data_municipios_pivot.rent_sqm_median > 0]

#### Create the table if does not exists already

# use the inspector to check if the secciones table exists already
# give the user the chance to drop it and recreate in case it is there
if (inspector.has_table(dataset_municipios.name)):
    print("Table `data_municipios` already exists. Type (drop) to drop and recreate, otherwise it will be kept")
    dropSelection = input()
    if dropSelection == "drop":
        dataset_municipios.drop(cnx)
        data_municipios_pivot.to_sql('data_municipios', cnx)
        print("Table `data_municipios` recreated")
# try to create it if table is not there
else:
    data_municipios_pivot.to_sql('data_municipios', cnx)
    print("Table `data_municipios` created")
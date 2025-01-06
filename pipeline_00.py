import os
import gdown 
import duckdb 
import pandas as pd
from sqlalchemy import create_engine 
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame


def download_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok = True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)


def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith(".csv"):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    return arquivos_csv

def ler_csv(caminho_do_arquivo):
    dataframe_duckdb = duckdb.read_csv(caminho_do_arquivo)
    print(dataframe_duckdb)
    return dataframe_duckdb

def transformar(df: DuckDBPyRelation) -> DataFrame:
    df_transformado = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    return df_transformado

def salvar_no_postgres(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    df_duckdb.to_sql(tabela, con=engine,if_exists='append', index=False)


if __name__ == "__main__":
    url_pasta = 'https://drive.google.com/drive/folders/1D10FBl0V7qrz4TMGPefM2zSnuPE9AzpM'
    diretorio_local = './pasta_gdown'
    #download_google_drive(url_pasta, diretorio_local)
    arquivos = listar_arquivos_csv(diretorio_local)
    ler_csv(arquivos)

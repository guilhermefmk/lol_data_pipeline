from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect.blocks.system import Secret
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import datetime
import requests
import time
import os

@task(log_prints=True)
def get_headers() -> dict:
    secret_block = Secret.load("lol-api-key")
    key = secret_block.get()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": f"{key}"
    }
    print(secret_block)
    return headers

@task()
def extract_from_gcs() -> Path:
    '''Download trip data from GCS'''
    dia = datetime.datetime.now().day
    mes = datetime.datetime.now().month
    ano = datetime.datetime.now().year
    dataset_file = f'chalenger_{dia:02}-{mes:02}-{ano}'
    gcs_path = f"chalengers/{ano}/{mes:02}/{dia:02}/"
    local_path = Path(f"./extract_from_gcs/")
    local_path.mkdir(parents=True, exist_ok=True)
    gcp_block = GcsBucket.load("lol-datalake")
    gcp_block.get_directory(from_path=gcs_path, local_path=local_path)

    return Path(f"{local_path}/{gcs_path}/{dataset_file}.csv")

@task(log_prints=True)
def get_df_chalengers(data: Path) -> pd.DataFrame:
    '''Data cleaning example'''
    df = pd.read_csv(data, index_col=None)
    return df

@task()
def get_match_ids_for_summoner(puuid: str, headers) -> list:
    url = f'https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count=20'
    response = requests.get(url, headers=headers)
    match_ids_list = []
    for id in response.json():
        match_ids_list.append(id)
    return match_ids_list

@task()
def write_local(df: pd.DataFrame) -> str:
    '''Write DataFrame out locally as parquet file'''
    dia = datetime.datetime.now().day
    mes = datetime.datetime.now().month
    ano = datetime.datetime.now().year
    dataset_file = f'match_ids_{dia:02}-{mes:02}-{ano}'
    folder_path = f'match_ids/{ano}/{mes:02}/{dia:02}'
    dir_path = os.path.join(folder_path)
    path = os.path.join(dir_path, dataset_file + '.csv')


    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    df.to_csv(path, index=False)
    return path

@task()
def write_gcs(path: str):
    gcp_block = GcsBucket.load("lol-datalake")
    gcp_block.upload_from_path(
        from_path=f'{path}',
        to_path=path
    )



@flow(log_prints=True)
def etl_match_ids_to_gcs(list_match_ids) -> pd.DataFrame:
    '''Main ETL flow to load data into Big Query(datawarehouse)'''
    df = pd.DataFrame(list_match_ids, columns=['match_id'])
    path = write_local(df)
    write_gcs(path)
    return df

@flow(log_prints=True)
def etl_match_ids_to_gcs_subflow():
    header = get_headers()
    path = extract_from_gcs()
    df = get_df_chalengers(path)
    matches_ids_global_raw = []
    for index, row in df.iterrows():
        time.sleep(3)
        matches_ids_especific_summoner = get_match_ids_for_summoner(row['puuid'], header)
        matches_ids_global_raw.extend(matches_ids_especific_summoner)
    matches_ids_global_unique = set(matches_ids_global_raw)
    etl_match_ids_to_gcs(matches_ids_global_unique)



if __name__ == "__main__":
    etl_match_ids_to_gcs_subflow()
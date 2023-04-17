
import requests
import pandas as pd
import time
from prefect.blocks.system import Secret
from prefect.filesystems import GCS
from prefect_gcp import GcpCredentials, GcsBucket
from prefect import flow, task
import os
import datetime

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

@task(log_prints=True)
def get_entries(header: dict) -> str:
    response = requests.get('https://br1.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/CHALLENGER/I?page=1', headers=header)
    print(response.text)
    return response.text

@task()
def fetch(api_response_text: str) -> pd.DataFrame:
    df = pd.read_json(api_response_text,orient='records')
    return df


@task()
def get_list_puuid(df: pd.DataFrame, header) -> list:
    
    puuids = []
    for index, row in df.iterrows():
        url = f'https://br1.api.riotgames.com/lol/summoner/v4/summoners/{row["summonerId"]}'
        time.sleep(3)
        response = requests.get(url, headers=header)
        response_json = response.json()
        puuid = response_json['puuid']
        puuids.append(puuid)
    
    return puuids

def assign_df(df: pd.DataFrame, puuids: list) -> pd.DataFrame:
    df = df.assign(puuid=puuids)
    return df

@task()
def write_local(df: pd.DataFrame) -> str:
    '''Write DataFrame out locally as parquet file'''
    dia = datetime.datetime.now().day
    mes = datetime.datetime.now().month
    ano = datetime.datetime.now().year
    dataset_file = f'chalenger_{dia:02}-{mes:02}-{ano:02}'
    folder_path = f'chalengers/{ano}/{mes}/{dia}'
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
def etl_api_to_gcs() -> pd.DataFrame:
    header = get_headers()
    entries = get_entries(header)
    df = fetch(entries)
    puuids = get_list_puuid(df,header)
    df_puuids = assign_df(df,puuids)
    path = write_local(df_puuids)
    write_gcs(path)




if __name__ == '__main__':
    etl_api_to_gcs()
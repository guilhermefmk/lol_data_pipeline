from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import datetime
from prefect.blocks.system import Secret
import time
import requests

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
    '''Download match ids from gcs'''
    dia = datetime.datetime.now().day
    mes = datetime.datetime.now().month
    ano = datetime.datetime.now().year
    dataset_file = f'match_ids_{dia:02}-{mes:02}-{ano:02}'
    gcs_path = f"match_ids/{ano:02}/{mes:02}/{dia:02}"
    local_path = Path(f"./extract_from_gcs/")
    local_path.mkdir(parents=True, exist_ok=True)
    gcp_block = GcsBucket.load("lol-datalake")
    gcp_block.get_directory(from_path=gcs_path, local_path=local_path)

    return Path(f"{local_path}/{gcs_path}/{dataset_file}.csv")

@task(log_prints=True)
def fetch(data: Path) -> pd.DataFrame:
    '''Data cleaning example'''
    df = pd.read_csv(data, index_col=None)
    return df

@task()
def get_match_data(match_id: str, header: dict):
    url = f'https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}'
    response = requests.get(url, headers=header)
    dados = response.json()
    return dados

@task()
def struct_dict(dados) -> list:
    game_id = dados['info']['gameId']
    game_mode = dados['info']['gameMode']
    game_type = dados['info']['gameType']
    participantes = dados['info']['participants']

    lista_participantes = []
    for p in participantes:
        d = {'gameId': game_id, 'gameMode': game_mode, 'gameType': game_type}
        d.update(p)
        lista_participantes.append(d)
    return lista_participantes

@task()
def get_main_df() -> pd.DataFrame:
    df = pd.DataFrame(columns=['gameId', 'gameMode', 'gameType', 'allInPings', 'assistMePings', 'assists', 'baitPings', 'baronKills', 'basicPings', 'bountyLevel', 'challenges', 'champExperience', 'champLevel', 'championId', 'championName', 'championTransform', 'commandPings', 'consumablesPurchased', 'damageDealtToBuildings', 'damageDealtToObjectives', 'damageDealtToTurrets', 'damageSelfMitigated', 'dangerPings', 'deaths', 'detectorWardsPlaced', 'doubleKills', 'dragonKills', 'eligibleForProgression', 'enemyMissingPings', 'enemyVisionPings', 'firstBloodAssist', 'firstBloodKill', 'firstTowerAssist', 'firstTowerKill', 'gameEndedInEarlySurrender', 'gameEndedInSurrender', 'getBackPings', 'goldEarned', 'goldSpent', 'holdPings', 'individualPosition', 'inhibitorKills', 'inhibitorTakedowns', 'inhibitorsLost', 'item0', 'item1', 'item2', 'item3', 'item4', 'item5', 'item6', 'itemsPurchased', 'killingSprees', 'kills', 'lane', 'largestCriticalStrike', 'largestKillingSpree', 'largestMultiKill', 'longestTimeSpentLiving', 'magicDamageDealt', 'magicDamageDealtToChampions', 'magicDamageTaken', 'needVisionPings', 'neutralMinionsKilled', 'nexusKills', 'nexusLost', 'nexusTakedowns', 'objectivesStolen', 'objectivesStolenAssists', 'onMyWayPings', 'participantId', 'pentaKills', 'perks', 'physicalDamageDealt', 'physicalDamageDealtToChampions', 'physicalDamageTaken', 'profileIcon', 'pushPings', 'puuid', 'quadraKills', 'riotIdName', 'riotIdTagline', 'role', 'sightWardsBoughtInGame', 'spell1Casts', 'spell2Casts', 'spell3Casts', 'spell4Casts', 'summoner1Casts', 'summoner1Id', 'summoner2Casts', 'summoner2Id', 'summonerId', 'summonerLevel', 'summonerName', 'teamEarlySurrendered', 'teamId', 'teamPosition', 'timeCCingOthers', 'timePlayed', 'totalDamageDealt', 'totalDamageDealtToChampions', 'totalDamageShieldedOnTeammates', 'totalDamageTaken', 'totalHeal', 'totalHealsOnTeammates', 'totalMinionsKilled', 'totalTimeCCDealt', 'totalTimeSpentDead', 'totalUnitsHealed', 'tripleKills', 'trueDamageDealt', 'trueDamageDealtToChampions', 'trueDamageTaken', 'turretKills', 'turretTakedowns', 'turretsLost', 'unrealKills', 'visionClearedPings', 'visionScore', 'visionWardsBoughtInGame', 'wardsKilled', 'wardsPlaced', 'win', 'timestamp'])
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    '''Write DataFrame to BigQuery'''
    gcp_block = GcpCredentials.load("lol-credentials")
    df.to_gbq(
        destination_table= "lol.first",
        project_id= "lol-data-project-383712",
        credentials= gcp_block.get_credentials_from_service_account(),
        chunksize= 500_000,
        if_exists="append"
    )



@flow(log_prints=True)
def etl_gcs_to_bq(df) -> pd.DataFrame:
    '''Main ETL flow to load data into Big Query(datawarehouse)'''
    write_bq(df)
    return df

@flow()
def etl_sub_flow():
    header = get_headers()
    path = extract_from_gcs()
    df_ids = fetch(path)
    df_main = get_main_df()
    for index, row in df_ids.iterrows():
        time.sleep(1)
        json_response = get_match_data(row['match_id'], header)
        dados_participantes = struct_dict(json_response)
        df = df_main._append(dados_participantes, ignore_index=True)
    df_main['timestamp'] = pd.Timestamp.now().strftime('%Y-%m-%d')
    etl_gcs_to_bq(df_main)


if __name__ == "__main__":
    etl_sub_flow()
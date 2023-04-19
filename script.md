prefect orion start
prefect agent start -q 'default'
http://127.0.0.1:4200/

prefect deployment build 01_etl_chalengers_to_gcs.py:etl_chalengers_to_gcs --name 01_chalengers_to_gcs --apply

prefect deployment build 02_etl_match_ids_to_gcs.py:etl_match_ids_to_gcs_subflow --name 02_match_ids_to_gcs --apply

prefect deployment build 03_etl_data_match_to_bq.py:etl_data_match_to_bq_subflow --name 03_data_match_to_bq --apply
import pandas as pd
import yaml
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import mlflow

def create_connection():

    load_dotenv()
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    db = os.environ.get('DB_DESTINATION_NAME')
    username = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')
    
    print(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}', connect_args={'sslmode':'require'})
    return conn

def get_data():
    mlflow.start_run()
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    conn = create_connection()
    data = pd.read_sql('select * from flats_churn', conn, index_col=params['index_col'])
    conn.dispose()

    os.makedirs('part2_dvc/data', exist_ok=True)
    data.to_csv('part2_dvc/data/initial_data.csv', index=None)

    mlflow.end_run()

if __name__ == '__main__':
    get_data()


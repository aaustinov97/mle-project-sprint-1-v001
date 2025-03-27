import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOG_FORMAT  = f'WB_DATA DAG - '

def create_table():
    from sqlalchemy import MetaData, Table, Column, String, Integer, Float, String, Numeric, UniqueConstraint, inspect

    metadata = MetaData()
    flats_churn_table = Table(
    'flats_churn',
        metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('flat_id', Integer),
    Column('price', Numeric),
    Column('target', Numeric),
    Column('floor', Integer),
    Column('kitchen_area', Float),
    Column('living_area', Float),
    Column('rooms', Integer),
    Column('is_apartment', String),
    Column('studio', String),
    Column('total_area', Float),
    Column('build_year', Integer),
    Column('build_age', Integer),
    Column('building_type_int', Integer),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('ceiling_height', Float),
    Column('flats_count', Integer),
    Column('floors_total', Integer),
    Column('has_elevator', String),
    UniqueConstraint('flat_id', name='unique_flat_id')
    )
    
    postgres_hook = PostgresHook('destination_db')
    engine = postgres_hook.get_sqlalchemy_engine()

    if not inspect(engine).has_table(flats_churn_table.name): 
        logging.info(LOG_FORMAT + 'Table created')
        metadata.create_all(engine)

def extract(**kwargs):
    logging.info(LOG_FORMAT + 'Start the extract part')
    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    logging.info(LOG_FORMAT + 'Connected to db')
    sql = f"""
    select f.id as flat_id, f.price, f.floor, f.kitchen_area, f.living_area, f.rooms, f.is_apartment, f.studio,
                            f.total_area,
                            b.build_year, b.building_type_int, b.latitude, b.longitude, b.ceiling_height,
                            b.flats_count, b.floors_total, b.has_elevator
    from buildings as b left join flats as f on
            f.building_id = b.id
    """
    data = pd.read_sql(sql, conn)
    conn.close()

    ti.xcom_push('extracted_data', data)

def load(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids="extract", key='extracted_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="flats_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['flat_id'],
        rows=data.values.tolist()
)
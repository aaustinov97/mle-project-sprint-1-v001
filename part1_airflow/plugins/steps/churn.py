import pandas as pd
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_table():
    from sqlalchemy import MetaData, Table, Column, String, Integer, Float, String, DateTime, UniqueConstraint, inspect

    metadata = MetaData()
    flats_churn_table = Table(
    'flats_churn',
        metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('flat_id', Integer),
    Column('price', Float),
    Column('floor', Integer),
    Column('kitchen_area', Float),
    Column('living_area', Float),
    Column('rooms', Integer),
    Column('is_apartment', String),
    Column('studio', String),
    Column('total_area', Float),
    Column('build_year', Integer),
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
        metadata.create_all(engine)

def extract(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    sql = f"""
    select f.id as flat_id, f.price, f.floor, f.kitchen_area, f.living_area, f.rooms, f.is_apartment, f.studio, f.total_area,
                            b.build_year, b.building_type_int, b.latitude, b.longitude, b.ceiling_height, b.flats_count,
                            b.floors_total, b.has_elevator
    from buildings as b left join flats as f on
            f.building_id = b.id
    """
    data = pd.read_sql(sql, conn)
    conn.close()

    ti.xcom_push('extracted_data', data)

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')
    
    # Удаление дубликатов
    feature_cols = data.columns.drop('flat_id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)
    
    # Заполнение пропусков - Отсутсвуют в датасете
    cols_to_replace = ['kitchen_area', 'living_area', 'total_area']
    num_cols =data[cols_to_replace]
    num_cols = num_cols.replace(0, np.nan)
    data[cols_to_replace] = num_cols

    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index

    for col in cols_with_nans:
        if data[col].dtype in [float, int]:
            fill_value = data[col].median()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]
        data[col] = data[col].fillna(fill_value)
    
    # Удаление выбросов
    num_cols = data.select_dtypes(['float', 'int']).columns
    num_cols = num_cols.drop(['latitude', 'longitude', 'building_type_int', 'build_year', 'flat_id'])
    threshold = 1.5
    potential_outliers = pd.DataFrame()

    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = threshold * IQR
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)

    outliers = potential_outliers.any(axis=1)
    data = data[~outliers].reset_index(drop=True)

    #TODO: Разобраться почему для price надо второй раз выполнять удаление
    num_cols = data[['price']].columns
    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = threshold * IQR
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)

    outliers = potential_outliers.any(axis=1)
    data = data[~outliers].reset_index(drop=True)
    #TODO: END

    ti.xcom_push('transformed_data', data)

def load(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids="transform", key='transformed_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="flats_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['flat_id'],
        rows=data.values.tolist()
)
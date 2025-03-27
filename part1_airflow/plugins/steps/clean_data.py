import pandas as pd
import numpy as np
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOG_FORMAT  = f'WB_DATA DAG - '


def transform(**kwargs):
    logging.info(LOG_FORMAT + 'Start the extract part')

    ti = kwargs['ti']

    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    logging.info(LOG_FORMAT + 'Connected to db')
    
    sql = f"""
    select * from flats_churn
    """
    data = pd.read_sql(sql, conn)
    conn.close()
    
    # Преобразование колонок
    data['target'] = data['price'].astype(float)

    # Преобразуем год постройки на возраст здания
    from datetime import datetime
    data['build_age'] = datetime.now().year - data['build_year']

    # Удаление дубликатов
    data = del_duplicate(data)

    # Заполнение пропусков - Отсутсвуют в датасете
    data = insert_empty_values(data)
    
    # Удаление выбросов
    data = clean_outliers(data)

    ti.xcom_push('transformed_data', data)

def load(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids="transform", key='transformed_data')
    hook = PostgresHook('destination_db')
    hook.run("DELETE FROM flats_churn;")
    hook.insert_rows(
        table="flats_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['flat_id'],
        rows=data.values.tolist()
)
    

def del_duplicate(data: pd.DataFrame) -> pd.DataFrame:
    feature_cols = data.columns.drop('flat_id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)

    return data

def insert_empty_values(data: pd.DataFrame) -> pd.DataFrame:
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

    return data

def clean_outliers(data: pd.DataFrame) -> pd.DataFrame:
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

    # num_cols = data[['price']].columns
    # for col in num_cols:
    #     Q1 = data[col].quantile(0.25)
    #     Q3 = data[col].quantile(0.75)
    #     IQR = Q3 - Q1
    #     margin = threshold * IQR
    #     lower = Q1 - margin
    #     upper = Q3 + margin
    #     potential_outliers[col] = ~data[col].between(lower, upper)

    # outliers = potential_outliers.any(axis=1)
    # data = data[~outliers].reset_index(drop=True)

    return data
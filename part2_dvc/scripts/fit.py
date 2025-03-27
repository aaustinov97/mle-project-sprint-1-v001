import pandas as pd
import yaml
import os
import joblib
import mlflow
import mlflow.sklearn
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler, MinMaxScaler
from catboost import CatBoostRegressor


def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)


    mlflow.set_tracking_uri("/home/mle_projects/mle-project-sprint-1-v001/part2_dvc/mlruns")
    mlflow.start_run()
    
    data = pd.read_csv('part2_dvc/data/initial_data.csv')

    cat_features = data.select_dtypes(include='object')
    potential_binary_features = cat_features.nunique() == 2
    binary_cat_features = cat_features[potential_binary_features[potential_binary_features].index]

    one_hot_encoder_features = pd.concat([binary_cat_features, data[['building_type_int']]], axis=1)
    min_max_columns = data[['build_age', 'floor', 'rooms', 'floors_total']]
    
    num_features = data.select_dtypes(['float'])
    num_features.drop(columns=['target',], inplace=True)

    preprocessor = ColumnTransformer(
        [
            ('one_hot_enc', OneHotEncoder(drop=params['one_hot_drop']), one_hot_encoder_features.columns.tolist()),
            ('minmax', MinMaxScaler(), min_max_columns.columns.tolist()),
            ('num', 'passthrough', num_features.columns.tolist()) # StandardScaler() не целесообразен при градиентном бустинге
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    model = CatBoostRegressor(
                iterations=params['iterations'], 
                depth=params['depth'], 
                learning_rate=params['learning_rate'], 
                l2_leaf_reg=params['l2_leaf_reg'], 
                random_seed=params['random_seed'],
                verbose=params['verbose']
                )

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    pipeline.fit(data, data[params['target_col']])
    os.makedirs('part2_dvc/models', exist_ok=True)
    joblib.dump(pipeline, "part2_dvc/models/fitted_model.pkl")

    
    mlflow.log_params(params)
    mlflow.sklearn.log_model(model, "model")
    
    mlflow.end_run()

if __name__ == '__main__':
    fit_model()
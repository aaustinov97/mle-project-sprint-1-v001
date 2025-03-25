import pandas as pd
import yaml
import os
import joblib
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler, MinMaxScaler
from catboost import CatBoostRegressor


def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

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
            ('num', StandardScaler(), num_features.columns.tolist())
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

if __name__ == '__main__':
    fit_model()
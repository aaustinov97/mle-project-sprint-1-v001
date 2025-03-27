from sklearn.model_selection import cross_validate, KFold
import yaml
import joblib
import pandas as pd
import os
import json
import mlflow

def evaluate_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    
    mlflow.set_tracking_uri("/home/mle_projects/mle-project-sprint-1-v001/part2_dvc/mlruns")
    mlflow.start_run()
    
    mlflow.log_params(params)

    pipeline = joblib.load('part2_dvc/models/fitted_model.pkl')
    data = pd.read_csv('part2_dvc/data/initial_data.csv')
    
    cv_strategy = KFold(n_splits=params['n_splits'], shuffle=params['shuffle'], random_state=params['random_seed'])

    cv_res = cross_validate(
        pipeline,
        data.drop(columns=[params['target_col']]),
        data[params['target_col']],
        cv=cv_strategy,
        n_jobs=params['n_jobs'],
        scoring=params['scoring']
    )
    
    for key, value in cv_res.items():
        cv_res[key] = round(value.mean(), 3)
        mlflow.log_metric(key, round(value.mean(), 3))

    os.makedirs('part2_dvc/cv_results', exist_ok=True)
    with open('part2_dvc/cv_results/cv_res.json', 'w') as f:
        json.dump(cv_res, f)

    mlflow.end_run()


if __name__ == '__main__':
    evaluate_model()
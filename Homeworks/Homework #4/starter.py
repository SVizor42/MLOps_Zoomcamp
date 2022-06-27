#!/usr/bin/env python
# coding: utf-8
import os
import click
import pickle
import pandas as pd


def load_model(model_path):
    with open(model_path, 'rb') as f_in:
        transformer, model = pickle.load(f_in)
    return (transformer, model)


def read_data(filename: str) -> pd.DataFrame:
    df = pd.read_parquet(filename)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    return df


def prepare_dictionaries(df: pd.DataFrame):
    categorical = ['PUlocationID', 'DOlocationID']
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    dicts = df[categorical].to_dict(orient='records')
    return dicts


def apply_model(taxi_type: str, year: int, month: int):
    input_file = f'https://nyc-tlc.s3.amazonaws.com/trip+data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'data/{taxi_type}/{year:04d}_{month:02d}_result.parquet'
    model_path = "model.bin"

    os.makedirs(f'data/{taxi_type}', exist_ok=True)

    print(f'Reading the data from {input_file}...')
    df = read_data(input_file)
    dicts = prepare_dictionaries(df)

    print(f'Loading the model...')
    dv, lr = load_model(model_path)

    print(f'Applying the model...')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)
    print(f'Mean predicted duration: {y_pred.mean():.3f}')

    print(f'Saving the result to {output_file}...')
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    df['prediction'] = y_pred
    df_result = df[['ride_id', 'prediction']]
    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )
    print('Done.')


@click.command()
@click.option("--taxi_type", default='fhv')
@click.option("--year", default=2021)
@click.option("--month", default=1)
def run(taxi_type: str, year: int, month: int):
    apply_model(taxi_type, year, month)


if __name__ == '__main__':
    run()

import os
import pandas as pd
from datetime import datetime


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)


data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (1, 1, dt(1, 2, 0), dt(2, 2, 1)),
]

columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
df_input = pd.DataFrame(data, columns=columns)

endpoint_url = os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')
options = {
    'client_kwargs': {
        'endpoint_url': endpoint_url
    }
}
input_file = 's3://nyc-duration/in/2021-01.parquet'

df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

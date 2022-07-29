import pandas as pd
from pandas.testing import assert_frame_equal
from datetime import datetime

from batch import prepare_data


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)


def test_prepare_data():
    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
    df = pd.DataFrame(data, columns=columns)
    categorical = ['PUlocationID', 'DOlocationID']
    
    actual_result = prepare_data(df, categorical)
    expected_result = pd.DataFrame(
        [
            ('-1', '-1', dt(1, 2), dt(1, 10), 8.0),
            ('1', '1', dt(1, 2), dt(1, 10), 8.0),
        ],
        columns=columns + ['duration']
    )

    assert_frame_equal(actual_result, expected_result)

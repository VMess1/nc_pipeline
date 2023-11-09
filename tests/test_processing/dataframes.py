import pandas as pd


def currency_dataframe():
    return pd.DataFrame(data={
        'currency_id': [1, 2, 3],
        'currency_code': ['GBP', 'USD', 'EUR'],
        'created_at': ['2022-11-03 14:20:49.962000',
                       '2022-11-03 14:20:49.962000',
                       '2022-11-03 14:20:49.962000'],
        'last_updated': ['2022-11-03 14:20:49.962000',
                         '2022-11-03 14:20:49.962000',
                         '2022-11-03 14:20:49.962000']
    })


def currency_dataframe_transformed():
    return pd.DataFrame(data={
        'currency_id': [1, 2, 3],
        'currency_code': ['GBP', 'USD', 'EUR'],
        'currency_name': ['British Pound Sterling',
                          'United States Dollar',
                          'Euro']
    })

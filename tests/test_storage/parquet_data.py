import pandas as pd


def currency_dataframe():
    return pd.DataFrame(data={
        'currency_id': [1, 2, 3],
        'currency_code': ['GBP', 'USD', 'EUR'],
        'currency_name': ['British Pound Sterling',
                          'United States Dollar',
                          'Euro']
    })


def dim_location_dataframe1():
    return pd.DataFrame(data={
        'location_id': [1, 2, 3],
        'address_line_1': ['Herzog Via', 'Alexie Cliffs', 'Sincere Fort'],
        'address_line_2': ['None', 'None', 'None'],
        'district': ['Avon1', 'Avon', 'Avon'],
        'city': ['New Patienceburgh1',
                 'New Patienceburgh',
                 'New Patienceburgh'],
        'postal_code': ['28441', '28441', '28441'],
        'country': ['Turkey1', 'Turkey', 'Turkey'],
        'phone': ['1803 637401', '1803 637401', '1803 637401']
    })


def dim_location_dataframe2():
    return pd.DataFrame(data={
        'location_id': [4, 5],
        'address_line_1': ['Daniel Daniels', 'David Davids'],
        'address_line_2': ['None', 'None'],
        'district': ['Avon1', 'Babylon'],
        'city': ['New Patienceburgh1', 'Luton'],
        'postal_code': ['28441', '12345'],
        'country': ['Brazil', 'England'],
        'phone': ['1803 637401', '1234 567890']
    })

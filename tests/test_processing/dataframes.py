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


def counterparty_dataframe():
    return pd.DataFrame(data={
        'counterparty_id': [1, 2, 3],
        'counterparty_legal_name': ['Fahey and Sons',
                                    'Leannon, Predovic and Morar',
                                    'Armstrong Inc'],
        'legal_address_id': [1, 2, 3],
        'commercial_contact': ['Micheal Toy',
                               'Melba Sanford',
                               'Jane Wiza'],
        'delivery_contact': ['Mrs. Lucy Runolfsdottir',
                             'Jean Hane III',
                             'Myra Kovacek'],
        'created_at': ['2022-11-03 14:20:51.563000',
                       '2022-11-03 14:20:51.563000',
                       '2022-11-03 14:20:51.563000'],
        'last_updated': ['2022-11-03 14:20:51.563000',
                         '2022-11-03 14:20:51.563000',
                         '2022-11-03 14:20:51.563000']
    })


def address_dataframe():
    return pd.DataFrame(
        data={
            'address_id': [
                1,
                2,
                3],
            'address_line_1': [
                'Herzog Via',
                'Alexie Cliffs',
                'Sincere Fort'],
            'address_line_2': [
                'None',
                'None',
                'None'],
            'district': [
                'Avon1',
                'Avon',
                'Avon'],
            'city': [
                'New Patienceburgh1',
                'New Patienceburgh',
                'New Patienceburgh'],
            'postal_code': [
                '28441',
                '28441',
                '28441'],
            'country': [
                'Turkey1',
                'Turkey',
                'Turkey'],
            'phone': [
                '1803 637401',
                '1803 637401',
                '1803 637401'],
            'created_at': [
                '2022-11-03 14:20:51.563000',
                '2022-11-03 14:20:51.563000',
                '2022-11-03 14:20:51.563000'],
            'last_updated': [
                '2022-11-03 14:20:51.563000',
                '2022-11-03 14:20:51.563000',
                '2022-11-03 14:20:51.563000']})


def dim_counterparty_dataframe():
    return pd.DataFrame(data={
        'counterparty_id': [1, 2, 3],
        'counterparty_legal_name': ['Fahey and Sons',
                                    'Leannon, Predovic and Morar',
                                    'Armstrong Inc'],
        'counterparty_legal_address_line_1': ['Herzog Via',
                                              'Alexie Cliffs',
                                              'Sincere Fort'],
        'counterparty_legal_address_line_2': ['None', 'None', 'None'],
        'counterparty_legal_district': ['Avon1', 'Avon', 'Avon'],
        'counterparty_legal_city': ['New Patienceburgh1',
                                    'New Patienceburgh',
                                    'New Patienceburgh'],
        'counterparty_legal_postal_code': ['28441', '28441', '28441'],
        'counterparty_legal_country': ['Turkey1',
                                       'Turkey',
                                       'Turkey'],
        'counterparty_legal_phone_number': ['1803 637401',
                                            '1803 637401',
                                            '1803 637401'],
    })


def dim_location_dataframe():
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

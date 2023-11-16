import pandas as pd
import datetime


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


def sales_order_dataframe():
    return pd.DataFrame(
        data={
            'sales_order_id': [
                5136,
                5137,
                5138],
            'created_at': [
                pd.Timestamp('2022-01-01T12'),
                pd.Timestamp('2022-02-01T12'),
                pd.Timestamp('2022-03-01T12')],
            'last_updated': [
                pd.Timestamp('2022-01-01T12'),
                pd.Timestamp('2022-02-01T12'),
                pd.Timestamp('2022-03-01T12')],
            'design_id': [
                51,
                52,
                53],
            'staff_id': [
                1,
                2,
                3],
            'counterparty_id': [
                1,
                2,
                3],
            'units_sold': [
                1,
                1,
                1],
            'unit_price': [
                9.99,
                10.99,
                11.99],
            'currency_id': [
                1,
                1,
                1],
            'agreed_delivery_date': [
                '2022-01-01',
                '2022-02-01',
                '2022-03-01'],
            'agreed_payment_date': [
                '2022-01-01',
                '2022-02-01',
                '2022-03-01'],
            'agreed_delivery_location_id': [
                1,
                2,
                3]})


def fact_sales_dataframe():
    return pd.DataFrame(data={
        'sales_order_id': [5136, 5137, 5138],
        'created_date': [datetime.date(2022, 1, 1),
                         datetime.date(2022, 2, 1),
                         datetime.date(2022, 3, 1)],
        'created_time': [datetime.time(12, 0, 0),
                         datetime.time(12, 0, 0),
                         datetime.time(12, 0, 0)],
        'last_updated_date': [datetime.date(2022, 1, 1),
                              datetime.date(2022, 2, 1),
                              datetime.date(2022, 3, 1)],
        'last_updated_time': [datetime.time(12, 0, 0),
                              datetime.time(12, 0, 0),
                              datetime.time(12, 0, 0)],
        'design_id': [51, 52, 53],
        'sales_staff_id': [1, 2, 3],
        'counterparty_id': [1, 2, 3],
        'units_sold': [1, 1, 1],
        'unit_price': [9.99, 10.99, 11.99],
        'currency_id': [1, 1, 1],
        'agreed_delivery_date': [datetime.date(2022, 1, 1),
                                 datetime.date(2022, 2, 1),
                                 datetime.date(2022, 3, 1)],
        'agreed_payment_date': [datetime.date(2022, 1, 1),
                                datetime.date(2022, 2, 1),
                                datetime.date(2022, 3, 1)],
        'agreed_delivery_location_id': [1, 2, 3]
    })

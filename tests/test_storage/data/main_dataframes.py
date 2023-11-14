import pandas as pd


def dim_location_df0():
    return pd.DataFrame(data={
            'location_id': [1, 2, 3],
            'address_line_1': ['street_1', 'street_2', 'street_3'],
            'address_line_2': ['place_1', None, None],
            'district': ['district_1', 'district_2', 'district_3'],
            'city': ['city_1',
                     'city_2',
                     'city_3'],
            'postal_code': ['A111AA', 'B222BB', 'C333CC'],
            'country': ['country_1', 'country_2', 'country_3'],
            'phone': ['1803 637401', '1803 637402', '1803 637403']
        })

def dim_location_df1():
    return pd.DataFrame(data={
        'location_id': [4, 5, 6],
        'address_line_1': ['street_4', 'street_5', 'street_6'],
        'address_line_2': ['place_4', 'place_5', 'place_6'],
        'district': ['district_4', 'district_5', 'district_6'],
        'city': ['city_4',
                 'city_5',
                 'city_6'],
        'postal_code': ['28444', '28445', '28446'],
        'country': ['country_4', 'country_5', 'country_6'],
        'phone': ['1803 637404', '1803 637405', '1803 637406']
    })


def dim_location_df2():
    return pd.DataFrame(data={
        'location_id': [7, 8],
        'address_line_1': ['street_7', 'street_8'],
        'address_line_2': ['place_7', 'place_8'],
        'district': ['district_7', 'district_8'],
        'city': ['city_7',
                 'city_8'],
        'postal_code': ['28447', '28448'],
        'country': ['country_7', 'country_8'],
        'phone': ['1803 637407', '1803 637408']
    })

def dim_location_df3():
    return pd.DataFrame(data={
        'location_id': [4],
        'address_line_1': ['street_9'],
        'address_line_2': ['place_9'],
        'district': ['district_9'],
        'city': ['city_9'],
        'postal_code': ['28449'],
        'country': ['country_9'],
        'phone': ['1803 637409']
    })
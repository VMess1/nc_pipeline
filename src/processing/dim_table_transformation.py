import pandas as pd
from datetime import date, timedelta


def dim_remove_dates(data):
    '''
    takes a dataframe and returns one with the columns
    'created_at' and 'last_updated' columns removed.
    '''
    transformed_data = data.drop(columns="created_at", inplace=False, axis=1)
    transformed_data = transformed_data.drop(
        columns='last_updated', inplace=False, axis=1)

    return transformed_data


def dim_join_department(staff_data, departments_data):
    '''
    takes staff dataframe and full department dataframe and
    joins department info to staff data on department_id,
    returning a staff dataframe with department names
    '''
    new_staff_data = dim_remove_dates(staff_data.copy())
    new_departments_data = dim_remove_dates(departments_data.copy())
    result = pd.merge(new_staff_data, new_departments_data, on="department_id")

    result = result.drop(columns=['manager', 'department_id'],
                         axis=1)

    return result


def dim_insert_currency_name(data):
    '''
    Takes a currency dataframe and returns a new dataframe
    with currency codes added.
    '''
    new_data = data.copy()
    currency_codes = {
        'USD': 'United States Dollar',
        'EUR': 'Euro',
        'JPY': 'Japanese Yen',
        'GBP': 'British Pound Sterling',
        'CHF': 'Swiss Franc',
        'AUD': 'Australian Dollar',
        'NZD': 'New Zealand Dollar',
        'CAD': 'Canadian Dolar'
    }

    def get_code(name):
        '''
        takes a currency name and returns a code
        '''
        return currency_codes.get(name, 'Invalid')

    code_entries = new_data['currency_code'].tolist()
    name_entries = [get_code(name) for name in code_entries]
    new_data['currency_name'] = name_entries
    return new_data


def join_address(counterparty_df, address_df):
    '''
    Takes a counterparty dataframe and a full address dataframe,
    joins address info to counterparty table.
    Returns counterparty dataframe with full address info.
    '''
    renamed_cp = counterparty_df.rename(
        {'legal_address_id': 'address_id'}, axis='columns')
    merged_df = pd.merge(renamed_cp, address_df, on='address_id')
    merged_df = merged_df.drop(columns="created_at_y", inplace=False, axis=1)
    merged_df = merged_df.drop(columns="last_updated_y", inplace=False, axis=1)
    merged_df = merged_df.drop(columns="created_at_x", inplace=False, axis=1)
    merged_df = merged_df.drop(columns="last_updated_x", inplace=False, axis=1)
    merged_df = merged_df.drop(
        columns="commercial_contact",
        inplace=False,
        axis=1)
    merged_df = merged_df.drop(
        columns="delivery_contact",
        inplace=False,
        axis=1)
    merged_df = merged_df.drop(columns="address_id", inplace=False, axis=1)
    merged_df = merged_df.rename(
        {
            'address_line_1': 'counterparty_legal_address_line_1',
            'address_line_2': 'counterparty_legal_address_line_2',
            'district': 'counterparty_legal_district',
            'city': 'counterparty_legal_city',
            'postal_code': 'counterparty_legal_postal_code',
            'country': 'counterparty_legal_country',
            'phone': 'counterparty_legal_phone_number'},
        axis='columns')

    return merged_df


def dim_locationtf(address_df):
    '''takes address dataframe and renames column to
    transform into location dataframe'''
    renamed_df = address_df.rename(
        {'address_id': 'location_id'}, axis='columns')
    renamed_df = dim_remove_dates(renamed_df)
    return renamed_df


def dim_date_tf():
    '''
    Returns a dataframe with info for every day's date
    from 01/01/2020 to 31/12/2049
    '''
    date_df = pd.date_range(date(2020, 1, 1), date(
        2050, 1, 1) - timedelta(days=1), freq='d')
    date_df = date_df.to_frame(index=False, name='date_id')
    date_df['year'] = pd.DatetimeIndex(date_df['date_id']).year
    date_df['month'] = pd.DatetimeIndex(date_df['date_id']).month
    date_df['day'] = pd.DatetimeIndex(date_df['date_id']).day
    date_df['day_of_week'] = pd.DatetimeIndex(date_df['date_id']).dayofweek
    s = pd.Series(
        pd.date_range(
            date(
                2020,
                1,
                1),
            date(
                2050,
                1,
                1) -
            timedelta(
                days=1),
            freq='d'))
    date_df['day_name'] = s.dt.day_name()
    date_df['month_name'] = s.dt.month_name()
    date_df['quarter'] = pd.DatetimeIndex(date_df['date_id']).quarter
    return date_df

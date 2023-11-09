import pandas as pd
from src.processing.dim_table_transformation import dim_remove_dates
from datetime import date, timedelta


def join_address(counterparty_df, address_df):
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


def dim_location(address_df):
    renamed_df = address_df.rename(
        {'address_id': 'location_id'}, axis='columns')
    renamed_df = dim_remove_dates(renamed_df)
    return renamed_df


def dim_date():
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

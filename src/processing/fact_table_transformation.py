import pandas as pd


def fact_sales_order_tf(sale_order):
    '''
    takes sales_order dataframe and transforms it,
    returning fact_sales_order format dataframe
    '''
    new_dataframe = sale_order
    # new_dataframe['sales_record_id'] = new_dataframe.reset_index().index
    new_dataframe = new_dataframe.rename(
        {'staff_id': 'sales_staff_id'}, axis='columns')
    new_dataframe['created_date'] = pd.DatetimeIndex(
        sale_order['created_at']).date
    new_dataframe['created_time'] = pd.DatetimeIndex(
        sale_order['created_at']).time
    new_dataframe['last_updated_date'] = pd.DatetimeIndex(
        sale_order['last_updated']).date
    new_dataframe['last_updated_time'] = pd.DatetimeIndex(
        sale_order['last_updated']).time
    new_dataframe = new_dataframe.drop(
        columns="created_at", inplace=False, axis=1)
    new_dataframe = new_dataframe.drop(
        columns="last_updated", inplace=False, axis=1)
    new_dataframe['agreed_delivery_date'] = pd.DatetimeIndex(
        sale_order['agreed_delivery_date']).date
    new_dataframe['agreed_payment_date'] = pd.DatetimeIndex(
        sale_order['agreed_payment_date']).date
    return new_dataframe

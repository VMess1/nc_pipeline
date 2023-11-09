import pandas as pd

def fact_sales_order(sale_order):
    new_dataframe = sale_order
    new_dataframe['sales_record_id'] =new_dataframe.reset_index().index
    new_dataframe.rename({'staff_id': 'sales_staff_id'}, axis='columns')
    
    print(new_dataframe)
    return sale_order
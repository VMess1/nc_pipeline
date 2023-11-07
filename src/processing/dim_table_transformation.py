import pandas as pd


def dim_remove_dates(data):
    data.drop('created_at', inplace=True, axis=1)
    data.drop('last_updated', inplace=True, axis=1)
    return data


def dim_join_department(staff_data, departments_data, timestamp):
    result = pd.merge(staff_data, departments_data, on="department_id")

    # new_df = staff_data.set_index('department_id').join(
    # departments_data.set_index('department_id'))
    return result

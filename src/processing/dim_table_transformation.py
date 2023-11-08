import pandas as pd


def dim_remove_dates(data):
    transformed_data = data.drop(columns="created_at", inplace=False, axis=1)
    transformed_data = transformed_data.drop(
        columns='last_updated', inplace=False, axis=1)
    return transformed_data


def dim_join_department(staff_data, departments_data, timestamp):
    result = pd.merge(staff_data, departments_data, on="department_id")

    # new_df = staff_data.set_index('department_id').join(
    # departments_data.set_index('department_id'))
    return result

import pandas as pd


def dim_remove_dates(data):
    transformed_data = data.drop(columns="created_at", inplace=False, axis=1)
    transformed_data = transformed_data.drop(
        columns='last_updated', inplace=False, axis=1)
    return transformed_data


def dim_join_department(staff_data, departments_data):
    new_staff_data = dim_remove_dates(staff_data.copy())
    new_departments_data = dim_remove_dates(departments_data.copy())
    result = pd.merge(new_staff_data, new_departments_data, on="department_id")

    result = result.drop(columns=['manager', 'department_id'],
                         axis=1)

    return result


def dim_insert_currency_name(data):
    '''Add currency codes to currency table'''

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
        return currency_codes.get(name, 'Invalid')

    code_entries = new_data['currency_code'].tolist()
    name_entries = [get_code(name) for name in code_entries]
    new_data['currency_name'] = name_entries
    return new_data

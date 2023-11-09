import pandas as pd
from dataframes import currency_dataframe

from src.processing.dim_table_transformation import (
    dim_remove_dates,
    dim_insert_currency_name
)


class TestDimRemoveDates:
    def test_dates_are_removed_from_basic_tables(self):
        test_dataframe = currency_dataframe()
        expected_dataframe = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR']
        })
        assert dim_remove_dates(test_dataframe).equals(expected_dataframe)

    # def test_join(self):
    #     test_df_department = pd.DataFrame(data={
    #         'department_id': [1, 2, 3],
    #         'department_name': ['Sales', 'Purchasing', 'Production'],
    #         'location': ['Manchester', 'Manchester', 'Leeds'],
    #         'manager': ['Richard Roma', 'Naomi Lapaglia', 'Chester Ming'],
    #         'created_at': ['2022-11-03 14:20:49.962000',
    # '2022-11-03 14:20:49.962000', '2022-11-03 14:20:49.962000'],
    #         'last_updated': ['2022-11-03 14:20:49.962000',
    # '2022-11-03 14:20:49.962000', '2022-11-03 14:20:49.962000']
    #     })
    #     test_df_staff = pd.DataFrame(data={
    #         'staff_id': [1, 2, 3],
    #         'first_name': ['Jeremie', 'Deron', 'Jeanette'],
    #         'last_name': ['Franey', 'Beier', 'Erdman'],
    #         'department_id': [2, 3, 2],
    #         'email_address': ['jeremie.franey@terrifictotes.com',
    #  'deron.beier@terrifictotes.com', 'jeanette.erdman@terrifictotes.com'],
    #         'created_at': ['2022-11-03 14:20:49.962000',
    #  '2022-11-03 14:20:49.962000', '2022-11-03 14:20:49.962000'],
    #         'last_updated': ['2022-11-03 14:20:49.962000',
    #  '2022-11-03 14:20:49.962000', '2022-11-03 14:20:49.962000']
    #     })
    #     test_df_dim_staff = pd.DataFrame(data={
    #         'staff_id': [1, 2, 3],
    #         'first_name': ['Jeremie', 'Deron', 'Jeanette'],
    #         'last_name': ['Franey', 'Beier', 'Erdman'],
    #         'department_id': [2, 3, 2],
    #         'email_address': ['jeremie.franey@terrifictotes.com',
    #  'deron.beier@terrifictotes.com', 'jeanette.erdman@terrifictotes.com'],
    #         'created_at': ['2022-11-03 14:20:49.962000',
    #  '2022-11-03 14:20:49.962000', '2022-11-03 14:20:49.962000'],
    #         'last_updated': ['2022-11-03 14:20:49.962000',
    #  '2022-11-03 14:20:49.962000', '2022-11-03 14:20:49.962000'],

    #     })

    #     res = dim_join_department(test_df_staff, test_df_department)
    #     pprint(res)


class TestDimInsertCurrencyName:
    def test_new_currency_table_includes_correct_currency_name(self):
        test_input = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR']
        })
        test_expected = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR'],
            'currency_name': ['British Pound Sterling',
                              'United States Dollar',
                              'Euro']
        })
        output = dim_insert_currency_name(test_input)
        assert output.equals(test_expected)

    def test_invalid_code_marked_as_invalid_in_currency_name(self):
        test_input = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'ABC']
        })
        test_expected = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'ABC'],
            'currency_name': ['British Pound Sterling',
                              'United States Dollar',
                              'Invalid']
        })
        output = dim_insert_currency_name(test_input)
        assert output.equals(test_expected)

    def test_does_not_mutate_input_dataframe(self):
        test_input = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR']
        })
        test_expected_input = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR']
        })
        output = dim_insert_currency_name(test_input)
        assert output is not test_input
        assert test_input.equals(test_expected_input)

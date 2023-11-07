import pandas as pd
from dataframes import currency_dataframe

from src.processing.dim_table_transformation import (
    dim_remove_dates,
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

import pandas as pd
from dataframes import (currency_dataframe,
                        counterparty_dataframe,
                        address_dataframe,
                        dim_counterparty_dataframe,
                        dim_location_dataframe)

from src.processing.dim_table_transformation import (
    dim_remove_dates,
    dim_insert_currency_name,
    dim_join_department,
    join_address,
    dim_locationtf,
    dim_date_tf
)


class TestDimRemoveDates:
    def test_dates_are_removed_from_basic_tables(self):
        test_dataframe = currency_dataframe()
        expected_dataframe = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR']
        })
        assert dim_remove_dates(test_dataframe).equals(expected_dataframe)


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


class TestDimJoinDepartmentId:
    def test_merges_2_tables_and_drop_2_tables(self):
        test_staff_table = pd.DataFrame(data={
            'staff_id': [1, 2, 3],
            'first_name': ['A', 'B', 'C'],
            'last_name': ['X', 'Y', 'Z'],
            'department_id': [3, 2, 1],
            'created_at': ['2022-12-12 15:15:15', '2023-12-12 15:15:15',
                           '2030-12-12 15:15:15'],
            'last_updated': ['2022-12-12 15:15:15', '2023-12-12 15:15:15',
                             '2030-12-12 15:15:15']
        })
        test_department_table = pd.DataFrame(data={
            'department_id': [1, 2, 3],
            'department_name': ['E', 'F', 'G'],
            'loctation': ['Luton', 'Wales', 'Essex'],
            'manager': ['Bob', 'Caron', 'Jeff'],
            'created_at': ['2022-12-12 15:15:15', '2023-12-12 15:15:15',
                           '2030-12-12 15:15:15'],
            'last_updated': ['2022-12-12 15:15:15', '2023-12-12 15:15:15',
                             '2030-12-12 15:15:15']
        })

        test_expected = pd.DataFrame(data={
            'staff_id': [1, 2, 3],
            'first_name': ['A', 'B', 'C'],
            'last_name': ['X', 'Y', 'Z'],
            'department_name': ['G', 'F', 'E'],
            'loctation': ['Essex', 'Wales', 'Luton']
        })
        output = dim_join_department(test_staff_table, test_department_table)
        assert output.equals(test_expected)


class TestCounterParty:
    def test_counterparty_address_tables_are_joined_correctly(self):
        test_counterparty = counterparty_dataframe()
        test_address = address_dataframe()
        test_transformed = dim_counterparty_dataframe()
        actual = join_address(test_counterparty, test_address)
        assert actual.equals(test_transformed)

    def test_data_is_not_mutated_by_join_address(self):
        test_counterparty = counterparty_dataframe()
        test_address = address_dataframe()
        join_address(test_counterparty, test_address)
        original_counterparty = counterparty_dataframe()
        original_address = address_dataframe()
        assert test_counterparty.equals(original_counterparty)
        assert test_address.equals(original_address)


class TestLocation:
    def test_location_table_is_formed_correctly(self):
        test_address = address_dataframe()
        test_transformed = dim_locationtf(test_address)
        actual = dim_location_dataframe()
        assert actual.equals(test_transformed)

    def test_data_is_not_mutated_by_dim_location(self):
        test_address = address_dataframe()
        dim_locationtf(test_address)
        original_address = address_dataframe()
        assert test_address.equals(original_address)


class TestDimDate:
    def test_start_date_of_dim_date_is_1_1_2020(self):
        test_df = dim_date_tf()
        assert test_df.iloc[0]['date_id'] == pd.Timestamp(
            year=2020, month=1, day=1)

    def test_first_row_has_appropriate_info(self):
        test_df = dim_date_tf()
        assert test_df.iloc[0]['year'] == 2020
        assert test_df.iloc[0]['month'] == 1
        assert test_df.iloc[0]['day'] == 1
        assert test_df.iloc[0]['day_of_week'] == 2
        assert test_df.iloc[0]['day_name'] == "Wednesday"
        assert test_df.iloc[0]['month_name'] == "January"
        assert test_df.iloc[0]['quarter'] == 1

    def test_last_row_has_appropriate_info(self):
        test_df = dim_date_tf()
        assert test_df.iloc[10957]['year'] == 2049
        assert test_df.iloc[10957]['month'] == 12
        assert test_df.iloc[10957]['day'] == 31
        assert test_df.iloc[10957]['day_of_week'] == 4
        assert test_df.iloc[10957]['day_name'] == "Friday"
        assert test_df.iloc[10957]['month_name'] == "December"
        assert test_df.iloc[10957]['quarter'] == 4

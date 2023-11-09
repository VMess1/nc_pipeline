import pandas as pd
from src.processing.dim_table_counterparty import (join_address,
                                                   dim_location,
                                                   dim_date)
from tests.test_processing.dataframes import (counterparty_dataframe,
                                              address_dataframe,
                                              dim_counterparty_dataframe,
                                              dim_location_dataframe)


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
        test_transformed = dim_location(test_address)
        actual = dim_location_dataframe()
        assert actual.equals(test_transformed)

    def test_data_is_not_mutated_by_dim_location(self):
        test_address = address_dataframe()
        dim_location(test_address)
        original_address = address_dataframe()
        assert test_address.equals(original_address)


class TestDimDate:
    def test_start_date_of_dim_date_is_1_1_2020(self):
        test_df = dim_date()
        assert test_df.iloc[0]['date_id'] == pd.Timestamp(
            year=2020, month=1, day=1)

    def test_first_row_has_appropriate_info(self):
        test_df = dim_date()
        assert test_df.iloc[0]['year'] == 2020
        assert test_df.iloc[0]['month'] == 1
        assert test_df.iloc[0]['day'] == 1
        assert test_df.iloc[0]['day_of_week'] == 2
        assert test_df.iloc[0]['day_name'] == "Wednesday"
        assert test_df.iloc[0]['month_name'] == "January"
        assert test_df.iloc[0]['quarter'] == 1

    def test_last_row_has_appropriate_info(self):
        test_df = dim_date()
        assert test_df.iloc[10957]['year'] == 2049
        assert test_df.iloc[10957]['month'] == 12
        assert test_df.iloc[10957]['day'] == 31
        assert test_df.iloc[10957]['day_of_week'] == 4
        assert test_df.iloc[10957]['day_name'] == "Friday"
        assert test_df.iloc[10957]['month_name'] == "December"
        assert test_df.iloc[10957]['quarter'] == 4

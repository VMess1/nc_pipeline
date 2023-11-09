from src.processing.fact_table_transformation import fact_sales_order
from tests.test_processing.dataframes import (sales_order_dataframe,
                                              fact_sales_dataframe)


def test_fact_sales_order_transformed_correctly():
    test_sale_order = sales_order_dataframe()
    actual = fact_sales_order(test_sale_order).sort_index().sort_index(axis=1)
    expected = fact_sales_dataframe().sort_index().sort_index(axis=1)
    assert actual.equals(expected)

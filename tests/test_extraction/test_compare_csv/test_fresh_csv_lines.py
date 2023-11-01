from src.extraction.fresh_csv_lines import get_fresh
from tests.test_extraction.test_compare_csv import strings


def test_check_comparison_with_1_old_file_gives_difference():
    new_string = strings.new_string()
    existing_string = strings.existing_string_1()
    new_csv = get_fresh(new_string, existing_string)
    difference = strings.difference_1()
    assert new_csv == difference


def test_comparison_with_2_files_gives_difference():
    new_string = strings.new_string()
    existing_string1 = strings.existing_string_1()
    existing_string2 = strings.existing_string_2()
    new_csv = get_fresh(new_string, existing_string1, existing_string2)
    difference = strings.difference_2()
    assert new_csv == difference

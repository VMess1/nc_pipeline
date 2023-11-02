'''
Function returns new csv lines as string.
Function compares number of lines in previous csvs.
Function takes new and previous csvs as string arguments.
'''


def get_fresh(new_csv, *args):
    total_previous_lines = 0
    for i in args:
        total_previous_lines += i.count('\n') - 2
    headers = "\n".join(new_csv.split("\n")[:2])
    new_data = "\n".join(new_csv.split("\n")[total_previous_lines + 2:])
    csv_to_upload = headers + '\n' + new_data
    return csv_to_upload

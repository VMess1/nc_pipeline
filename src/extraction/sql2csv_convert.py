'''
Function to return string in csv format.
Function requires table name, data list
and header list as retreived by sql query.
'''


def convert_to_csv(table_name, data, headers):
    the_goods = ''
    the_goods += table_name + '\n'
    for index, collumn in enumerate(headers):
        if index == len(headers) - 1:
            the_goods += f'{collumn[0]}\n'
        else:
            the_goods += f'{collumn[0]}, '
    for index, datum in enumerate(data):
        for index, dat in enumerate(datum):
            if index == len(datum) - 1:
                the_goods += f'{dat}\n'
            else:
                the_goods += f'{dat}, '
    return the_goods

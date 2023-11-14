def new_string():
    payment_result = 'payment_id;created_at;last_updated;transaction_id;'
    payment_result += 'counterparty_id;payment_amount;currency_id;'
    payment_result += 'payment_type_id;paid;payment_date;'
    payment_result += 'company_ac_number;counterparty_ac_number\n'
    payment_result += '2;2022-11-03 14:20:52;2022-11-03 14:20:52;2;15;'
    payment_result += '552548.62;2;3;False;2022-11-04;67305075;'
    payment_result += '31622269\n'
    payment_result += '3;2022-11-03 14:20:52;2022-11-03 14:20:52;3;18;'
    payment_result += '205952.22;3;1;False;2022-11-03;81718079;'
    payment_result += '47839086\n'
    payment_result += '5;2022-11-03 14:20:52;2022-11-03 14:20:52;5;17;'
    payment_result += '57067.20;2;3;False;2022-11-06;66213052;'
    payment_result += '91659548\n'
    payment_result += '8;2022-11-03 14:20:52;2022-11-03 14:20:52;8;2;'
    payment_result += '254007.12;3;3;False;2022-11-05;32948439;'
    payment_result += '90135525\n'
    payment_result += '16;2022-11-03 14:20:52;2022-11-03 14:20:52;16;15;'
    payment_result += '250459.52;2;1;False;2022-11-05;34445327;'
    payment_result += '71673373\n'
    payment_result += '2;2022-11-03 14:20:52;2022-11-03 14:20:52;2;15;'
    payment_result += '552548.62;2;3;False;2022-11-04;67305075;'
    payment_result += '31622269\n'
    return payment_result


def existing_string_1():
    payment_result = "payment\n"
    payment_result += "payment_id, created_at, last_updated, transaction_id, "
    payment_result += "counterparty_id, payment_amount, currency_id, "
    payment_result += "payment_type_id, paid, payment_date, "
    payment_result += "company_ac_number, counterparty_ac_number\n"
    payment_result += "2, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 2, 15, "
    payment_result += "552548.62, 2, 3, False, 2022-11-04, 67305075, "
    payment_result += "31622269\n"
    payment_result += "3, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 3, 18, "
    payment_result += "205952.22, 3, 1, False, 2022-11-03, 81718079, "
    payment_result += "47839086\n"
    payment_result += "5, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 5, 17, "
    payment_result += "57067.20, 2, 3, False, 2022-11-06, 66213052, "
    payment_result += "91659548\n"
    payment_result += "8, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 8, 2, "
    payment_result += "254007.12, 3, 3, False, 2022-11-05, 32948439, "
    payment_result += "90135525\n"
    return payment_result


def difference_1():
    payment_result = """payment\n
    payment_id, created_at, last_updated, transaction_id,
     counterparty_id, payment_amount, currency_id,
     payment_type_id, paid, payment_date,
     company_ac_number, counterparty_ac_number\n
    16, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 16, 15,
     250459.52, 2, 1, False, 2022-11-05, 34445327,
     71673373\n
    2, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 2, 15,
     552548.62, 2, 3, False, 2022-11-04, 67305075,
     31622269\n"""
    return payment_result


def existing_string_2():
    payment_result = """payment\n
    payment_id, created_at, last_updated, transaction_id,
     counterparty_id, payment_amount, currency_id,
     payment_type_id, paid, payment_date,
     company_ac_number, counterparty_ac_number\n
    16, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 16, 15,
     250459.52, 2, 1, False, 2022-11-05, 34445327,
     71673373\n"""
    return payment_result


def difference_2():
    payment_result = """payment\n"
    payment_id, created_at, last_updated, transaction_id,
     counterparty_id, payment_amount, currency_id,
     payment_type_id, paid, payment_date,
     company_ac_number, counterparty_ac_number\n
    2, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 2, 15,
     552548.62, 2, 3, False, 2022-11-04, 67305075,
     31622269\n"""
    return payment_result


def currency_string():
    return '''currency_id;currency_code;created_at;last_updated\n
    1;GBP;2022-11-03 14:20:52;2022-11-03 14:20:52\n
    2;USD;2022-11-03 14:20:52;2022-11-03 14:20:52\n
    3;EUR;2022-11-03 14:20:52;2022-11-03 14:20:52\n
    '''

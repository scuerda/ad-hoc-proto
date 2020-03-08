import argparse
import csv

from collections import namedtuple
from functools import partial
from io import BufferedReader
from struct import Struct, error as StructError

from typing import Callable, Iterator, Optional, Union

# Events in file
CREDIT = 0
DEBIT = 1
START_AUTOPAY = 2
STOP_AUTOPAY = 3

TRANSACTIONS = [CREDIT, DEBIT]

record_type_map = {
    0: 'CREDIT',
    1: 'DEBIT',
    2: 'START_AUTOPAY',
    3: 'STOP_AUTOPAY'
}

# Struct definitions
header_struct = Struct('>4cbl')
type_struct = Struct('b')
transaction_struct = Struct('>biqd')
instruction_struct = Struct('>biq')

Transaction = namedtuple('Transaction', 'event timestamp user_id amount')
Instruction = namedtuple('Instruction', 'event timestamp user_id')
Header = namedtuple('Header', 'mainframe_name version record_count')


def __reader(struct_definition: Struct, output_tuple: namedtuple, data) -> namedtuple:
    """Helper function for building out struct reader functions"""
    return output_tuple._make(struct_definition.unpack(data))


transaction_reader = partial(__reader, transaction_struct, Transaction)
instruction_reader = partial(__reader, instruction_struct, Instruction)


def header_reader(data) -> Header:
    header_row = header_struct.unpack(data.read(header_struct.size))
    # mainframe name comes back as 4 separate values in the tuple, so we need to combine and decode
    mainframe_name = (header_row[0] + header_row[1] + header_row[2] + header_row[3]).decode('UTF-8')
    version = header_row[4]
    record_count = header_row[5]
    return Header(mainframe_name, version, record_count)


def row_parser(buffered_data: BufferedReader) -> Optional[Iterator[Union[Transaction, Instruction]]]:
    """Parse a BufferedReader object and convert data to namedtuples"""
    while True:
        try:
            row_type = type_struct.unpack(buffered_data.peek(type_struct.size))[0]
        except (StructError, ValueError):
            return

        if row_type in TRANSACTIONS:
            res = transaction_reader(buffered_data.read(transaction_struct.size))
        else:
            res = instruction_reader(buffered_data.read(instruction_struct.size))

        yield res


def get_data(buffered_data, user_account_tracker: Callable = None, file_writer: Callable = None) -> tuple:
    """Data processing function that calculates summary stats and optionally tracks balances and writes output to csv"""

    # initialize values
    debits = 0.0
    credits = 0.0
    starts = 0
    stops = 0

    for row in row_parser(buffered_data):
        # we might have a non-conforming row, so we can skip for now
        if row is None:
            continue

        if file_writer:
            file_writer(row)

        if user_account_tracker:
            user_account_tracker(row)

        if row.event == CREDIT:
            credits += row.amount
        elif row.event == DEBIT:
            debits += row.amount
        elif row.event == START_AUTOPAY:
            starts += 1
        elif row.event == STOP_AUTOPAY:
            stops += 1

    return debits, credits, starts, stops


def _row_writer(dict_writer, fields, data):
    """Helper function for turning a namedtuple record into a dict and remapping the record type to human readable"""
    row = dict(zip(fields, data))
    row['Record type'] = record_type_map.get(row['Record type'])

    dict_writer.writerow(row)


def setup_output_file(open_file) -> Callable:
    fields = ["Record type", "Unix timestamp", "user ID", "amount in dollars"]
    writer = csv.DictWriter(open_file, fieldnames=fields)
    writer.writeheader()

    # We bind _row_writer using partial so that we can return it and pass it in to get_data and write as we parse
    output_file_writer = partial(_row_writer, writer, fields)
    return output_file_writer


class UserBalance:
    """Helper class for storing a running transaction balance for a given user id

    Returns a callable so that it can be used as if it were a function.

    Example:

        >>> balance_tracker = UserBalance(user_id=123)
        >>> balance_tracker.balance
        0

        >>> Row = namedtuples('Row', "event user_id amount")
        >>> row = Row(0,123,50.50)
        >>> row.amount
        50.50

        >>> balance_tracker(row)
        >>> balance_tracker.balance
        50.50

    """

    def __init__(self, user_id: int):
        self.user_id = user_id
        self.balance = 0

    def __call__(self, row: namedtuple):
        if row.user_id != self.user_id:
            return

        if row.event == CREDIT:
            self.balance += row.amount
        elif row.event == DEBIT:
            self.balance -= row.amount


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse binary transaction data for fun and profit")
    parser.add_argument('--input', help='File path for binary file to process', required=True)
    parser.add_argument('--user_id', default=None, help='User ID to calculate balance for')
    parser.add_argument('--output', default=None, help='File path for CSV output')
    parser.add_argument('--no_stats', action='store_false', help='Flag for suppressing summary stats')

    args = parser.parse_args()

    run = args.input is not None
    if run:
        datafile = open(args.input, "rb")
        buffered_data = BufferedReader(datafile, 1)

        mainframe_name, version, record_count = header_reader(buffered_data)
        print(f'{mainframe_name} Version {version} -- {record_count} Total Records\n')

        try:
            user_id = args.user_id
            report_file = open(args.output, 'w') if args.output else None

            # Setup helpers if we get args
            report_file_writer = setup_output_file(report_file) if report_file else None
            user_balance_tracker = UserBalance(user_id) if user_id else None

            debits, credits, starts, stops = get_data(
                buffered_data,
                user_account_tracker=user_balance_tracker,
                file_writer=report_file_writer
            )

            if args.no_stats:
                print(
                    f"Total debits: ${debits}\nTotal credits: ${credits}\nTotal starts: {starts}\nTotal stops: {stops}"
                )

            if user_balance_tracker:
                print(f"\nFinal balance for user: {user_id}: ${user_balance_tracker.balance}")

        finally:
            datafile.close()
            if report_file:
                report_file.close()

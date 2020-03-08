import unittest

from io import BufferedReader, BytesIO
from struct import pack

from parse import (
    CREDIT,
    DEBIT,
    START_AUTOPAY,
    STOP_AUTOPAY,
    type_struct,
    transaction_struct,
    instruction_struct,
    row_parser,
    get_data,
    UserBalance
)


class TestStructParsing(unittest.TestCase):
    def test_event_type_mismatch(self):
        decoded, = type_struct.unpack(b'\x02')

        self.assertNotEqual(CREDIT, decoded)

    def test_event_type_parsing(self):
        expected_events = [
            (b'\x00', CREDIT),
            (b'\x01', DEBIT),
            (b'\x02', START_AUTOPAY),
            (b'\x03', STOP_AUTOPAY)
        ]

        for raw, expected in expected_events:
            with self.subTest(expected=expected):
                decoded, = type_struct.unpack(raw)
                self.assertEqual(decoded, expected)

    def test_instruction_parsing(self):
        record_type = START_AUTOPAY
        unix_timestamp = 1393108945
        user_id = 4136353673894269217
        row = record_type.to_bytes(1, 'big') + unix_timestamp.to_bytes(4, 'big') + user_id.to_bytes(8, 'big')
        result = instruction_struct.unpack(row)

        self.assertEqual((record_type, unix_timestamp, user_id), result)

    def test_transaction_parsing(self):
        record_type = CREDIT
        unix_timestamp = 1393108945
        user_id = 4136353673894269217
        amount = 604.274335557087
        row = (
            record_type.to_bytes(1, 'big') +
            unix_timestamp.to_bytes(4, 'big') +
            user_id.to_bytes(8, 'big') +
            pack('>d', amount)  # a bit of a hack b/c reasons
        )
        result = transaction_struct.unpack(row)

        self.assertEqual((record_type, unix_timestamp, user_id, amount), result)


class TestDataParsing(unittest.TestCase):
    def setUp(self):
        record_type = START_AUTOPAY
        unix_timestamp = 1393108329
        self.user_id = 4136353673894269217
        self.row = record_type.to_bytes(1, 'big') + unix_timestamp.to_bytes(4, 'big') + self.user_id.to_bytes(8, 'big')

        record_type = CREDIT
        unix_timestamp = 1393108945
        self.amount = 604.274335557087
        self.row_2 = (
            record_type.to_bytes(1, 'big') +
            unix_timestamp.to_bytes(4, 'big') +
            self.user_id.to_bytes(8, 'big') +
            pack('>d', self.amount)  # a bit of a hack b/c reasons
        )
        rows = self.row + self.row_2
        self.buffered_rows = BufferedReader(BytesIO(rows), 1)

    def test_row_parser_decodes_binary_data(self):
        parsed_rows = [row for row in row_parser(self.buffered_rows)]

        self.assertEqual(2, len(parsed_rows))
        self.assertEqual(parsed_rows[0].user_id, self.user_id)
        self.assertEqual(parsed_rows[0].event, START_AUTOPAY)
        self.assertEqual(parsed_rows[1].user_id, self.user_id)
        self.assertEqual(parsed_rows[1].event, CREDIT)

    def test_get_data_calculates_summary_stats(self):
        debits, credits, starts, stops = get_data(self.buffered_rows)

        self.assertEqual(debits, 0.0)
        self.assertEqual(credits, self.amount)
        self.assertEqual(starts, 1)
        self.assertEqual(stops, 0)

    def test_user_balance_is_updated_from_row_data(self):
        balance_tracker = UserBalance(self.user_id)
        self.assertEqual(balance_tracker.balance, 0)
        get_data(self.buffered_rows,  user_account_tracker=balance_tracker)

        self.assertEqual(balance_tracker.balance, self.amount)

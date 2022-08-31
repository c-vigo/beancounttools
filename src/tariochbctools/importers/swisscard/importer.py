import re
from datetime import datetime, timedelta
import unidecode

import camelot
from dateutil.parser import parse
from pandas import concat
from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from pathlib import Path
import csv


def get_statement_summary(file_name):
    # Statement date
    header = camelot.read_pdf(
        file_name,
        pages='1',
        flavor='stream',
        table_areas=['60,710,270,620']
    )
    date = None
    for index, row in header[0].df.iterrows():
        try:
            # Parse the header
            if "Statement date" in row[0]:
                date = parse(row[1].strip(), dayfirst=True).date()
                break
        except ValueError:
            pass
    else:
        raise ValueError

    # Balance
    table = camelot.read_pdf(
        file_name,
        pages='1',
        flavor='stream',
        table_areas=['50,480,560,462']
    )
    for index, row in table[0].df.iterrows():
        try:
            # Parse the line
            your_payment = D(row[1].replace("'", "").replace("CHF", ""))
            total_transactions = D(row[2].replace("'", "").replace("CHF", ""))
            new_balance = D(row[3].replace("'", "").replace("CHF", ""))
            return date, your_payment, total_transactions, new_balance
        except ValueError:
            pass

    raise ValueError


def parse_pdf_to_csv(pdf_file_name: str, csv_file_name: str):
    # Parse header
    statement_date, your_payment, total_transactions, new_balance = get_statement_summary(pdf_file_name)

    # Parse entries
    accumulated_cashflow = D(0)
    payments = D(0)
    entries = []
    table1 = camelot.read_pdf(
        pdf_file_name,
        pages='1',
        flavor='stream',
        table_areas=['50,380,560,50'],
        columns=['120,530']
    )
    table2 = camelot.read_pdf(
        pdf_file_name,
        pages='2-end',
        flavor='stream',
        table_areas=['50,800,560,50'],
        columns=['120,530']
    )

    df = concat([table1[0].df, table2[0].df])

    # Transactions
    for index, row in df.iterrows():
        try:
            # Parse row
            date = parse(row[0].strip(), dayfirst=True).date()
            desc = unidecode.unidecode(row[1].replace("\n", " "))
            cash_flow = -D(row[2].replace("'", ""))
            if "YOUR PAYMENT" in desc:
                cash_flow = -cash_flow
                payments = payments + cash_flow
            else:
                accumulated_cashflow = accumulated_cashflow - cash_flow
            entries.append([date, desc, cash_flow])

        except ValueError:
            pass

    assert (payments == your_payment)
    assert (accumulated_cashflow == total_transactions)

    # Save to CSV file
    with open(csv_file_name, 'wt') as f:
        # Header
        f.write('Date;Description;Amount\n')

        # Balance
        f.write('{};BALANCE;{}\n'.format(statement_date, new_balance))

        # Transactions
        for entry in entries:
            f.write('{};{};{}\n'.format(*entry))


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Cembra Card Statement PDF files."""

    def __init__(self, regexps, account):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.currency = "CHF"

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        entries = []

        # Parse the PDF to a CSV file
        csv_file = Path(file.name).with_suffix('.csv')
        if not csv_file.is_file():
            parse_pdf_to_csv(file.name, str(csv_file))

        # Read the CSV file
        with open(str(csv_file), 'r') as csvfile:
            reader = csv.reader(
                csvfile,
                delimiter=";"
            )
            rows = list(reader)

        # Balance
        entries.append(data.Balance(
            data.new_metadata(file.name, 0),
            parse(rows[1][0].strip(), dayfirst=False).date(),
            self.account,
            amount.Amount(-D(rows[1][2]), self.currency),
            None,
            None
        ))

        # Transactions
        for row in rows[2:]:
            date = parse(row[0].strip(), dayfirst=False).date()
            desc = unidecode.unidecode(row[1].replace("\n", " "))
            cash_flow = D(row[2].replace("'", ""))
            meta = data.new_metadata(file.name, 0)
            entries.append(data.Transaction(
                meta,
                date,
                "*",
                "",
                desc,
                data.EMPTY_SET,
                data.EMPTY_SET,
                [data.Posting(self.account, amount.Amount(D(cash_flow), self.currency), None, None, None, None)],
            ))

        return entries

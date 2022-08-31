import itertools
import re
from datetime import datetime, timedelta
from unicodedata import normalize

import camelot
from dateutil.parser import parse
from pandas import concat
from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from pathlib import Path
from typing import List, Union
import csv
# import matplotlib


def cleanDecimal(formatted_number):
    return D(formatted_number.replace(",", ""))


def parse_pdf(pdf_file_name: str) -> List[List[Union[datetime.date, D, str]]]:
    # Parse PDF file
    columns = ['120,287,340,430,500']
    table1 = camelot.read_pdf(
        pdf_file_name,
        pages='1',
        flavor='stream',
        table_areas=['55,543,550,110'],
        columns=columns
    )
    table2 = camelot.read_pdf(
        pdf_file_name,
        pages='2-end',
        flavor='stream',
        table_areas=['55,597,550,110'],
        columns=columns
    )
    tables = [table1, table2]

    # Loop over pages filling a list with transactions
    transactions = []
    for table in itertools.chain(*tables):
        # Plot for debugging, requires matplotlib
        # camelot.plot(table, kind='contour').show()

        # Loop over rows
        for index, row in table.df.iterrows():
            date, desc, card, debit, credit, balance = tuple(row)

            # Transaction date
            try:
                date = datetime.strptime(date, "%d.%m.%Y").date()
            except Exception:
                # A description spans over two lines?
                if not date and not card and not debit and not credit and not balance:
                    transactions[-1][2] += ' ' + normalize("NFKD", desc)
                continue

            # Transaction amount
            value = - cleanDecimal(debit) if debit else cleanDecimal(credit)
            transactions.append([date, value, normalize("NFKD", desc)])

    return transactions


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Reka Statement PDF files."""

    def __init__(self, regexps, account):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.currency = "CHF"

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        entries = []

        # Parse the PDF
        transactions = parse_pdf(file.name)

        for transaction in transactions:
            entries.append(data.Transaction(
                data.new_metadata(file.name, 0),
                transaction[0],
                "*",
                "",
                transaction[2],
                data.EMPTY_SET,
                data.EMPTY_SET,
                [data.Posting(self.account, amount.Amount(D(transaction[1]), self.currency), None, None, None, None)],
            ))

        return entries

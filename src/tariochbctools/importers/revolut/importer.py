import csv
import logging
from datetime import timedelta
from io import StringIO

from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Revolut CSV files."""

    def __init__(self, regexps, account, fee_account, currency):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.fee_account = fee_account
        self.currency = currency

    def name(self):
        return super().name() + self.account

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        entries = []
        has_balance = False

        with StringIO(file.contents()) as csvfile:
            reader = csv.DictReader(
                csvfile,
                [
                    "Type",
                    "Product",
                    "StartedDate",
                    "CompletedDate",
                    "Description",
                    "Amount",
                    "Fee",
                    "Currency",
                    "State",
                    "Balance"
                ],
                delimiter=",",
                skipinitialspace=True,
            )
            next(reader)
            for index, row in enumerate(reader):
                try:
                    meta = data.new_metadata(file.name, index)
                    book_date = parse(row['StartedDate'].strip()).date()
                    description = row["Type"].strip() + ' ' + row["Description"].strip()
                    cash_flow = amount.Amount(D(row["Amount"]) - D(row["Fee"]), row["Currency"])

                    # Process entry
                    entry = data.Transaction(
                        meta,
                        book_date,
                        "*",
                        "",
                        description,
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        [data.Posting(self.account, cash_flow, None, None, None, None)],
                    )
                    entries.append(entry)

                    # Update balance
                    balance = data.Balance(
                        meta,
                        book_date + timedelta(days=1),
                        self.account,
                        amount.Amount(D(row["Balance"]), self.currency),
                        None,
                        None
                    )

                except Exception as e:
                    logging.warning(e)
                    continue
        # Append balance
        # entries.append(balance)
        return entries

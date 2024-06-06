import csv
from io import StringIO

from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Neon CSV files."""

    def __init__(self, regexps, account, map={}):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.map = map

    def name(self):
        return super().name() + self.account

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries):
        entries = []

        with open(file.name, 'r', encoding='utf8') as csvfile:
            reader = csv.DictReader(
                csvfile,
                ["Date", "Amount", "Original amount", "Original currency", "Exchange rate", "Description", "Subject",
                 "Category", "Tags", "Wise", "Spaces"],
                delimiter=";"
            )
            rows = list(reader)[1:]

        for index, row in enumerate(reversed(rows)):
            try:
                # Parse transaction
                meta = data.new_metadata(file.name, index)
                book_date = parse(row['Date'].strip()).date()
                amt = amount.Amount(D(row["Amount"]), "CHF")
                metakv = {
                    "category": row["Category"],
                }
                if row["Original currency"] != "":
                    metakv["original_currency"] = row["Original currency"]
                    metakv["original_amount"] = row["Original amount"]
                    metakv["exchange_rate"] = row["Exchange rate"]

                meta_posting = data.new_metadata(file.name, 0, metakv)
                description = row["Description"].strip()
                if description in self.map:
                    payee = self.map[description][0]
                    note = self.map[description][1]
                else:
                    payee = ''
                    note = description

                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    payee,
                    note,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.account, amt, None, None, None, meta_posting),
                    ],
                ))

            except BaseException as e:
                raise Warning('Error parsing line {}\n{}'.format(row, e))

        return entries

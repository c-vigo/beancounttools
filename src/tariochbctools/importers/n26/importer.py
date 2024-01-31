import csv
from io import StringIO

from beancount.core import amount, data, position
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for N26 CSV files."""

    def __init__(self, regexps, account):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account

    def name(self):
        return super().name() + self.account

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries):
        entries = []

        with open(file.name, 'r', encoding='utf8') as csvfile:
            reader = csv.DictReader(
                csvfile,
                [
                    "Date",
                    "Payee",
                    "Account number",
                    "Transaction type",
                    "Payment reference",
                    "Amount (EUR)",
                    "Amount (Foreign Currency)",
                    "Type Foreign Currency",
                    "Exchange Rate"
                ],
                delimiter=","
            )
            rows = list(reader)[1:]

        for index, row in enumerate(rows):
            try:
                # Parse transaction
                meta = data.new_metadata(file.name, index)
                book_date = parse(row['Date'].strip()).date()
                payee = row["Payee"].strip()
                description = row["Payment reference"].strip()
                amt_eur = amount.Amount(D(row["Amount (EUR)"]), "EUR")

                foreign_currency = row["Type Foreign Currency"]
                cost_spec = None
                if foreign_currency and foreign_currency != "EUR":
                    cost_spec = position.CostSpec(None, D(row["Amount (Foreign Currency)"]), foreign_currency, None, None, None)

                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    payee,
                    description,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.account, amt_eur, cost_spec, None, None, None),
                    ],
                ))

            except BaseException as e:
                raise Warning('Error parsing line {}\n{} from file {}'.format(row, e, file.name))

        return entries

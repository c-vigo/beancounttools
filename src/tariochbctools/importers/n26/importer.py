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
                    "Booking Date",
                    "Value Date",
                    "Partner Name",
                    "Partner Iban",
                    "Type",
                    "Payment Reference",
                    "Account Name",
                    "Amount (EUR)",
                    "Original Amount",
                    "Original Currency",
                    "Exchange Rate"
                ],
                delimiter=","
            )
            rows = list(reader)[1:]

        for index, row in enumerate(rows):
            try:
                # Parse transaction
                meta = data.new_metadata(file.name, index)
                book_date = parse(row['Booking Date'].strip()).date()
                payee = row["Partner Name"].strip()
                description = row["Payment Reference"].strip() if row["Payment Reference"] else ""
                units = amount.Amount(D(row["Amount (EUR)"]), "EUR")
                cost = None

                #original_currency = row["Original Currency"]
                #if original_currency and original_currency != "EUR":
                #    units = amount.Amount(D(row["Original Amount"]), original_currency)
                #    cost = position.Cost(D(row["Amount (EUR)"]), 'EUR', None, None)
                #else:
                #    units = amount.Amount(D(row["Amount (EUR)"]), "EUR")
                #    cost = None

                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    payee,
                    description,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.account, units, cost, None, None, None),
                    ],
                ))

            except BaseException as e:
                raise Warning('Error parsing line {}\n{} from file {}'.format(row, e, file.name))

        return entries

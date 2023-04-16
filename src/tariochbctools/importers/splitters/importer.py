import csv
import logging
from datetime import timedelta
from io import StringIO

from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse


class SplitserImporter(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for custom Splitser CSV files."""

    def __init__(self, regexps, account):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account

    def name(self):
        return super().name() + self.account

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        entries = []

        with StringIO(file.contents()) as csvfile:
            reader = csv.DictReader(
                csvfile,
                delimiter=";",
                skipinitialspace=True,
            )
            for row in reader:
                try:
                    # Parse dictionary
                    meta = data.new_metadata(file.name, reader.line_num)
                    book_date = parse(row['Date'].strip()).date()
                    payee = row["Payee"].strip()
                    description = row["Concept"].strip()
                    cash_flow = amount.Amount(D(row["Value"]), row["Currency"].strip())

                    # Balance entry?
                    if payee == 'Balance':
                        entries.append(data.Balance(
                            meta,
                            book_date + timedelta(days=1),
                            self.account,
                            cash_flow,
                            None,
                            None
                        ))

                    # Process entry
                    else:
                        entries.append(data.Transaction(
                            meta,
                            book_date,
                            "*",
                            payee,
                            description,
                            data.EMPTY_SET,
                            data.EMPTY_SET,
                            [data.Posting(self.account, cash_flow, None, None, None, None)],
                        ))

                except Exception as e:
                    logging.warning(e)

        return entries

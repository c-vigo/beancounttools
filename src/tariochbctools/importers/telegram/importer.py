import csv
from typing import Any

from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Telegram downloader."""

    def __init__(self, regexps, account, map={}):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.map = map

    def name(self):
        return super().name() + self.account

    def file_account(self, file):
        return self.account

    def extract(self,
            file: {name},
            existing_entries: Any) -> list:
        entries = []

        with open(file.name, 'r', encoding='utf8') as csvfile:
            reader = csv.DictReader(
                csvfile,
                ['id', 'sender', 'message_date', 'transaction_date', 'account', 'payee', 'description', 'amount', 'currency', 'tag'],
                delimiter=";"
            )
            rows = list(reader)[1:]

        for index, row in enumerate(reversed(rows)):
            try:
                # Parse transaction
                meta = data.new_metadata(file.name, index)
                book_date = parse(row['transaction_date'].strip()).date()
                amt = amount.Amount(D(row["amount"]), row["currency"])
                note = row["description"].strip()
                payee = row["payee"].strip()
                tag = row["tag"].strip()
                if tag == '':
                    tag = data.EMPTY_SET
                else:
                    tag = {tag[1:]}

                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    payee,
                    note,
                    tag,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.account, amt, None, None, None, None),
                    ],
                ))

            except BaseException as e:
                raise Warning('Error parsing line {}\n{}'.format(row, e))

        return entries

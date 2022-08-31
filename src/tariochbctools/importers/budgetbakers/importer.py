import csv
import datetime
import subprocess
from io import StringIO
import re
from typing import List

from beancount.core import amount, data, position
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse
from enum import Enum
from typing import List, Dict


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for BudgetBakers CSV files."""

    def __init__(self, regexps, account_map, category_map):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account_map = account_map
        self.category_map = category_map

    def name(self):
        return super().name()

    def file_account(self, file):
        return self.parent_account

    def extract(self, file, existing_entries=None):
        entries = []

        with StringIO(file.contents()) as csvfile:
            reader = csv.DictReader(
                csvfile,
                delimiter=";",
                skipinitialspace=False,
            )
            rows = list(reader)

        for index, row in enumerate(reversed(rows)):
            try:
                # Parse transaction
                meta = data.new_metadata(file.name, index)
                book_date = parse(row['date'].strip()).date()
                debit_account = row['account'].strip()
                if debit_account not in self.account_map:
                    raise Warning('Account {} missing in map'.format(debit_account))
                debit_account = self.account_map[debit_account]
                type = row['type'].strip()
                category = row['category'].strip()
                payee = row['payee'].strip()
                cash_flow = amount.Amount(D(row['amount']), row['currency'].strip())
                note = row['note'].strip()
                label = row['labels'].strip().replace(" ", "")
                label = {label} if label else data.EMPTY_SET
                postings = [data.Posting(debit_account, cash_flow, None, None, None, None)]

                if category in self.category_map:
                    account = self.category_map[category]
                    if isinstance(account, str):
                        # Single expense account
                        postings.append(data.Posting(account, -cash_flow, None, None, None, None))
                    else:
                        # Multiple expense accounts
                        for acc, fraction in account:
                            partial_cash_flow = amount.Amount(D(fraction)*cash_flow[0], cash_flow[1])
                            postings.append(data.Posting(acc, -partial_cash_flow, None, None, None, None))

                    description = note
                else:
                    description = '{}: {}'.format(category, note)

                # Process entry
                entry = data.Transaction(
                    meta,
                    book_date,
                    "*",
                    payee,
                    description,
                    label,
                    data.EMPTY_SET,
                    postings,
                )
                entries.append(entry)

            except BaseException as e:
                raise Warning('Error parsing line {}\n{}'.format(row,e))
                continue

        return entries



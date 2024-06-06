import csv
import logging
from datetime import datetime, timedelta
from io import StringIO

from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse


def clean_decimal(formatted_number):
    return D(formatted_number.replace("'", ""))


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

                    # Process entry
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

                    # Settlement?
                    if description == 'Settle':
                        entries.append(data.Balance(
                            data.new_metadata(file.name, 0),
                            book_date + timedelta(days=1),
                            self.account,
                            amount.Amount(D(0), row["Currency"].strip()),
                            None,
                            None
                        ))


                except Exception as e:
                    logging.warning(e)

        return entries


class HouseHoldSplitWiseImporter(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for SplitWise CSV files."""
    def __init__(self, regexps, account, owner: str, partner: str, account_map: dict = None, tag: str = None):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.owner = owner
        self.partner = partner
        self.account_map = account_map if account_map is not None else dict()
        self.tag = {tag} if tag is not None else data.EMPTY_SET

    def name(self):
        return super().name() + self.account

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        entries = []

        # Read the CSV file
        with open(str(file.name), 'r') as csvfile:
            reader = csv.reader(
                csvfile,
                delimiter=","
            )
            rows = list(reader)

        # First row: header, sanity checks
        people = rows[0][5:]
        if len(people) != 2:
            raise RuntimeError('House-hold Splitwise requires two people')

        if self.owner not in people:
            raise RuntimeError('owner not found in the group')

        if self.partner not in people:
            raise RuntimeError('partner not found in the group')
        idx_owner = people.index(self.owner)
        idx_partner = people.index(self.partner)

        logging.debug('SplitWise Importer: owner found in pos. {} - {}'.format(idx_owner, people[idx_owner]))
        logging.debug('SplitWise Importer: partner found in pos. {} - {}'.format(idx_partner, people[idx_partner]))

        # Loop over transactions
        for index, row in enumerate(rows[2:]):
            # Split fields
            try:
                if idx_owner > idx_partner:
                    date, description, category, cost, currency, _, value = tuple(row)
                else:
                    date, description, category, cost, currency, value, _ = tuple(row)
            except ValueError:
                continue

            # Parse date
            date = datetime.strptime(date, "%Y-%m-%d").date()

            # Balance?
            if description == 'Total balance':
                entries.append(data.Balance(
                    data.new_metadata(file.name, index),
                    date,
                    self.account,
                    amount.Amount(clean_decimal(value), currency),
                    None,
                    None
                ))

            else:
                # Parse fields
                cost = clean_decimal(cost)
                value = clean_decimal(value)

                # Identify account from map
                exp_account = self.account_map.get(category, 'Expenses:FIXME')

                # Case 1: (partially) paid by owner
                if value > 0:
                    entries.append(data.Transaction(
                        data.new_metadata(file.name, index, {'category': category}),
                        date,
                        "*",
                        self.owner,
                        description,
                        self.tag,
                        data.EMPTY_SET,
                        [
                            data.Posting(self.account, amount.Amount(value, currency), None, None, None, None),
                            data.Posting(exp_account, amount.Amount(cost - value, currency), None, None, None, None)
                        ],
                    ))
                else:
                    entries.append(data.Transaction(
                        data.new_metadata(file.name, index, {'category': category}),
                        date,
                        "*",
                        self.partner,
                        description,
                        self.tag,
                        data.EMPTY_SET,
                        [
                            data.Posting(self.account, amount.Amount(value, currency), None, None, None, None),
                            data.Posting(exp_account, amount.Amount(-value, currency), None, None, None, None)
                        ],
                    ))

        return entries


class TripSplitWiseImporter(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for SplitWise CSV files."""
    def __init__(self, regexps, account, owner: str, expenses_account: str, tag: str = None):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.owner = owner
        self.expenses_account = expenses_account
        self.tag = {tag} if tag is not None else data.EMPTY_SET
        self.fixme_account = 'Expenses:FIXME'

    def name(self):
        return super().name() + self.account

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        entries = []

        # Read the CSV file
        with open(str(file.name), 'r') as csvfile:
            reader = csv.reader(
                csvfile,
                delimiter=","
            )
            rows = list(reader)

        # First row: header, sanity checks
        people = rows[0][5:]
        if len(people) < 2:
            raise RuntimeError('Trip Splitwise requires at least two people')

        if self.owner not in people:
            raise RuntimeError('owner not found in the group')

        idx_owner = people.index(self.owner)

        logging.debug('SplitWise Importer: owner found in pos. {} - {}'.format(idx_owner, people[idx_owner]))

        # Loop over transactions
        for index, row in enumerate(rows[2:]):
            # Split fields
            try:
                date, description, category, cost, currency, *splits = tuple(row)
            except ValueError:
                continue

            # Parse date
            date = datetime.strptime(date, "%Y-%m-%d").date()

            # Balance?
            if description == 'Total balance':
                entries.append(data.Balance(
                    data.new_metadata(file.name, index),
                    date,
                    self.account,
                    amount.Amount(clean_decimal(splits[idx_owner]), currency),
                    None,
                    None
                ))

            else:
                # Parse fields
                splits = [clean_decimal(split) for split in splits]
                cost = clean_decimal(cost)
                owner_balance = splits[idx_owner]
                others_balance = sum(splits) - owner_balance
                all_zeroes = all([split == D(0) for split in splits])

                # Case 1: no liability for anyone, fully paid by owner and just tracked here
                if all_zeroes:
                    continue
                    entries.append(data.Transaction(
                        data.new_metadata(file.name, index, {'category': category}),
                        date,
                        "*",
                        self.owner,
                        description,
                        self.tag,
                        data.EMPTY_SET,
                        [
                            data.Posting(self.expenses_account, amount.Amount(cost, currency), None, None, None, None)
                        ],
                    ))
                # Case 2: owner not ivolved, paid and owed by others
                elif owner_balance  == D(0) and others_balance == D(0):
                    continue
                # Case 3: negative balance for owner
                elif owner_balance < D(0):
                    entries.append(data.Transaction(
                        data.new_metadata(file.name, index, {'category': category}),
                        date,
                        "*",
                        "",
                        description,
                        self.tag,
                        data.EMPTY_SET,
                        [
                            data.Posting(self.account, amount.Amount(owner_balance, currency), None, None, None, None),
                            data.Posting(self.expenses_account, amount.Amount(-owner_balance, currency), None, None, None, None)
                        ],
                    ))
                # Case 4: positive balance for owner
                else:
                    entries.append(data.Transaction(
                        data.new_metadata(file.name, index, {'category': category}),
                        date,
                        "*",
                        "",
                        description,
                        self.tag,
                        data.EMPTY_SET,
                        [
                            data.Posting(self.account, amount.Amount(owner_balance, currency), None, None, None, None),
                            data.Posting(self.expenses_account, amount.Amount(cost-owner_balance, currency), None, None, None, None)
                        ],
                    ))

        return entries

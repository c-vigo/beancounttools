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


class TransactionType(Enum):
    Deposit = 'A deposit into the account'
    Removal = 'A withdrawal of capital'
    Buy = 'An investment'
    Sell = 'Payback of principal'
    Dividend = 'Return on the principal'
    Interest = 'Payment of interests (secondary market discounts, campaign bonus...)'
    Fees = 'Various fees (e.g. secondary market transactions)'
    Repurchase = 'Special: rincipal received from repurchase of small loan parts'

    @staticmethod
    def from_description(desc: str, value):
        if ' - discount/premium for secondary market transaction' in desc:
            if value > 0:
                return TransactionType.Interest
            else:
                raise ValueError('Negative discount?: {}'.format(desc))
        elif 'repurchase of small loan parts' in desc:
            return TransactionType.Repurchase
        elif ' - secondary market fee' in desc:
            return TransactionType.Fees
        elif ' - secondary market transaction' in desc:
            if value > 0:
                return TransactionType.Sell
            else:
                return TransactionType.Buy
        elif 'deposits' in desc:
            return TransactionType.Deposit
        elif 'withdrawal' in desc:
            return TransactionType.Removal
        elif ' - investment in loan' in desc:
            return TransactionType.Buy
        elif 'interest received' in desc or ' - late fees received' in desc:
            return TransactionType.Dividend
        elif 'principal received' in desc:
            return TransactionType.Sell
        elif 'refer a friend bonus' in desc or 'cashback bonus' in desc:
            if value > 0:
                return TransactionType.Interest
            else:
                raise ValueError('Negative bonus?: {}'.format(desc))
        elif 'deposit reversed' in desc:
            return TransactionType.Fees

        # Unknown
        raise ValueError('Invalid transaction details: {}'.format(desc))


class Transaction:
    # Attributes
    type: TransactionType
    value: D
    date: datetime.date

    def __init__(self, info: Dict[str, str]):
        self.value = D(info['Turnover'])
        self.type = TransactionType.from_description(
            info['Details'].strip().lower(),
            self.value
        )
        if self.type != TransactionType.Repurchase:
            self.date = parse(info['Date'].strip()).date()

class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Mintos CSV files."""

    def __init__(self, regexps, cash_account, loan_account, fees_account, pnl_account, external_account = None):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.cash_account = cash_account
        self.pnl_account = pnl_account
        self.loan_account = loan_account
        self.fees_account = fees_account
        self.external_account = external_account

    def name(self):
        return super().name() + self.cash_account

    def file_account(self, file):
        return self.parent_account

    def build_postings(self, accumulated_fees, accumulated_interest, accumulated_cashflow):
        postings: List[data.Posting] = []
        total = accumulated_cashflow + accumulated_fees + accumulated_interest
        if accumulated_interest != 0:
            postings.append(data.Posting(
                self.pnl_account, - amount.Amount(D(accumulated_interest), 'EUR'), None, None, None, None))
        if accumulated_fees != 0:
            postings.append(data.Posting(
                self.fees_account, - amount.Amount(D(accumulated_fees), 'EUR'), None, None, None, None))
        if accumulated_cashflow != 0:
            postings.append(data.Posting(
                self.loan_account, - amount.Amount(D(accumulated_cashflow), 'EUR'), None, None, None, None))
        if total != 0:
            postings.append(data.Posting(
                self.cash_account, amount.Amount(D(total), 'EUR'), None, None, None, None))

        return postings

    def extract(self, file, existing_entries=None):
        entries = []

        # Summary of entries only
        accumulated_fees = 0
        accumulated_interest = 0
        accumulated_cashflow = 0
        last_date = None
        last_index = None

        with StringIO(file.contents()) as csvfile:
            reader = csv.DictReader(
                csvfile,
               #  ["TransactionID","DateInput","Details","Turnover","Balance","Date","Value","Type","Note"],
                delimiter=",",
                skipinitialspace=False,
            )

            for last_index, row in enumerate(reader):
                # Parse transaction
                try:
                    transaction = Transaction(row)
                except BaseException as e:
                    raise Warning('Error parsing line {}\n{}'.format(row,e))
                    continue

                # Repurchase?
                if transaction.type == TransactionType.Repurchase:
                    accumulated_interest = accumulated_interest + transaction.value
                    continue

                # Accumulate?
                if transaction.type in [TransactionType.Interest, TransactionType.Dividend]:
                    accumulated_interest = accumulated_interest + transaction.value
                    last_date = transaction.date
                    continue
                if transaction.type == TransactionType.Fees:
                    accumulated_fees = accumulated_fees + transaction.value
                    last_date = transaction.date
                    continue
                if transaction.type in [TransactionType.Buy, TransactionType.Sell]:
                    accumulated_cashflow = accumulated_cashflow + transaction.value
                    last_date = transaction.date
                    continue

                # It's a deposit or removal, create entry with accumulated transactions and reset
                postings = self.build_postings(
                    accumulated_fees, accumulated_interest, accumulated_cashflow)
                accumulated_cashflow = accumulated_fees = accumulated_interest = 0
                if postings:
                    entries.append(data.Transaction(
                        data.new_metadata(file.name, last_index),
                        transaction.date,
                        "*",
                        "",
                        'Mintos - Summary',
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        postings,
                    ))

                # Now add entry for the deposit/removal
                postings = [
                    data.Posting(self.cash_account, amount.Amount(transaction.value, 'EUR'), None, None, None, None)]
                if self.external_account is not None:
                    postings.append(data.Posting(
                        self.external_account, - amount.Amount(transaction.value, 'EUR'), None, None, None, None))

                entries.append(data.Transaction(
                    data.new_metadata(file.name, last_index),
                    transaction.date,
                    "*",
                    "",
                    'Mintos - {}'.format('Deposit' if transaction.type == TransactionType.Deposit else 'Withdrawal'),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                ))

        # Last entry
        postings = self.build_postings(accumulated_fees, accumulated_interest, accumulated_cashflow)
        if postings:
            entries.append(data.Transaction(
                data.new_metadata(file.name, last_index),
                last_date,
                "*",
                "",
                "Mintos - Summary",
                data.EMPTY_SET,
                data.EMPTY_SET,
                postings,
            ))

        return entries



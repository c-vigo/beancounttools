import csv
from typing import Any

from beancount.core import amount, data, position
from beancount.core.number import D, round_to
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse
from os.path import basename
from pandas import read_excel
from warnings import simplefilter

simplefilter("ignore")


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Neon CSV files."""

    def __init__(self, regexps: str, account_cash: str, currency_cash: str,
                 account_stock: str, currency_stock: str, account_income: str
                 ):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account_cash = account_cash
        self.currency_cash = currency_cash
        self.account_stock = account_stock
        self.currency_stock = currency_stock
        self.account_income = account_income

    def name(self):
        return super().name() + self.account_stock

    def file_account(self, file):
        return self.account_stock

    def extract(self, file, existing_entries: Any) -> list:
        entries = []

        # Read Excel file into Pandas
        df = read_excel(
            io=file.name,
            header=5,
            names=['Date', 'Type', 'Price', 'Quantity'],
            dtype={
                'Date': 'str',
                'Type': 'str',
                'Price': 'str',
                'Quantity': 'str'
            },
            usecols="A,E,F,J",
            parse_dates=['Date'],
            engine="openpyxl"
        )
        grouped = df.groupby('Date')

        # Loop over group: one or more transactions in a single date
        for date, group in grouped:
            try:
                # Check same price for all transactions
                price: D = None
                for _, row in group.iterrows():
                    if price is None:
                        price = D(row['Price'])
                    elif price != D(row['Price']):
                        print('Warning: inconsistent price on', date, ': ', price, ' != ', row['Price'])
                        continue

                # Transaction details
                postings = []
                the_date = parse(str(date)).date()
                meta = data.new_metadata(
                    file.name,
                    group.first_valid_index
                )
                description = 'Purchase'

                # Postings details
                cash_flow: D = D()
                income_shares: D = D()
                income_cost: D = D()
                stock_shares: D = D()
                stock_cost: D = D()

                for index, row in group.iterrows():
                    # Parse posting details
                    quantity = D(row["Quantity"])
                    cost = price * quantity
                    Type = row['Type']

                    # Type of transaction
                    if Type == 'Purchase':
                        cash_flow = cash_flow - cost
                        stock_shares = stock_shares + quantity
                        stock_cost = stock_cost + cost
                    elif Type == 'Company match':
                        description = 'Purchase with company match'
                        income_shares = income_shares - quantity
                        income_cost = income_cost + cost
                        stock_shares = stock_shares + quantity
                        stock_cost = stock_cost + cost
                    else:
                        print('Warning: invalid posting type ', Type, ' on ', date)
                        continue

                # Postings
                if cash_flow != D():
                    postings.append(data.Posting(
                        self.account_cash,
                        amount.Amount(D(str(round(float(cash_flow), 2))), self.currency_cash),
                        None, None, None, None
                    ))
                if income_shares != D():
                    postings.append(data.Posting(
                        self.account_income,
                        amount.Amount(income_shares, self.currency_stock),
                        position.Cost(
                            price,
                            self.currency_cash,
                            the_date,
                            None),
                        None, None, None
                    ))
                if stock_shares != D():
                    postings.append(data.Posting(
                        self.account_stock,
                        amount.Amount(stock_shares, self.currency_stock),
                        position.Cost(
                            price,
                            self.currency_cash,
                            the_date,
                            None),
                        None, None, None
                    ))

                # Empty transaction?
                if not postings:
                    print('Warning: empty transaction on ', date)
                    continue

                # Create transaction
                entries.append(data.Transaction(
                    meta,
                    the_date,
                    '*',
                    'Roche Connect',
                    description,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings
                ))

            except BaseException:
                print('Could not parse group ', date)

        return entries

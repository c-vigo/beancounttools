import csv
import datetime
from io import StringIO
from typing import List

from beancount.core import amount, data, position
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse

from beanprice.price import fetch_cached_price
from beanprice.sources.ratesapi import Source


def build_sell_postings(
        entries,
        sec_account: str,
        cash_account: str,
        pnl_account: str,
        lot_date: datetime.date,
        shares: amount.Amount,
        cash_flow: amount.Amount,
        fx_rate: amount.Amount,
        currency: str
):
    lot = []
    pnl_cash_flow = -cash_flow[0]
    for entry in entries:
        # It is a transaction
        if not isinstance(entry, data.Transaction):
            continue
        entry: data.Transaction = entry

        # Up to given date
        if entry.date > lot_date:
            continue

        # Find this account
        for posting in entry.postings:
            if posting.account == sec_account:
                lot.append({
                    'units': posting.units,
                    'cost': posting.cost,
                    'date': entry.date
                })
                break
        else:
            continue

    # Sort by date and sell shares
    postings: List[data.Posting] = [data.Posting(cash_account, cash_flow, None, fx_rate, None, None)]
    lot = sorted(lot, key=lambda d: d['date'])
    for batch in lot:
        if batch['units'][0] + shares[0] >= 0:
            # Enough shares in this batch to cover the remaining shares
            # cost_per_share = batch['cost'][1] / batch['units'][0]
            # cost = position.Cost(D(cost_per_share), currency, None, None)
            postings.append(data.Posting(sec_account, shares, batch['cost'], None, None, None))

            # Update PnL
            pnl_cash_flow = pnl_cash_flow - D(batch['cost'][0] * shares[0] / (fx_rate[0] if fx_rate is not None else 1))
            postings.append(data.Posting(
                pnl_account, amount.Amount(D(pnl_cash_flow), 'CHF'), None, fx_rate, None, None)
            )
            return postings
        else:
            # Consume this batch, reduce outstanding shares and continue
            postings.append(data.Posting(sec_account, -batch['units'], batch['cost'], None, None, None))
            shares = amount.Amount(D(shares[0] - batch['units'][0]), shares[1])

            # Update PnL
            pnl_cash_flow = pnl_cash_flow + batch['cost'][0] / (fx_rate[0] if fx_rate is not None else 1)

    # If we reach here, we are trying to sell more shares than we had
    raise Warning('Inventory mismatch for account {}'.format(sec_account))


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for FinPension CSV files."""

    def __init__(self, regexps, parent_account, pnl_account, fees_account, securities):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.parent_account = parent_account
        self.pnl_account = pnl_account
        self.fees_account = fees_account
        self.securities = securities
        self.valid_categories = [
            'Buy',
            'Sell',
            'Flat-rate administrative fee',
            'Deposit',
            'Dividend',
            'Transfer'
        ]

    def name(self):
        return super().name() + self.parent_account

    def file_account(self, file):
        return self.parent_account

    def extract(self, file, existing_entries=None):
        entries = []
        sell_rows = []

        with StringIO(file.contents()) as csvfile:
            reader = csv.DictReader(
                csvfile,
                ["date", "category", "asset", "ISIN", "shares", "currency", "fxRate", "priceCHF", "cashFlow",
                 "balance"],
                delimiter=";",
                skipinitialspace=False,
            )
            rows = list(reader)[1:]

        for index, row in enumerate(reversed(rows)):
            # Parse
            book_date = parse(row["date"].strip()).date()
            meta = data.new_metadata(file.name, index)
            cashFlow = amount.Amount(D(row["cashFlow"]), "CHF")
            category = row["category"].strip()

            # Check category exists
            if category not in self.valid_categories:
                raise Warning('Unknown category {}'.format(category))

            # Fees, Deposits & Dividends
            if category == "Flat-rate administrative fee":
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "",
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                        data.Posting(self.fees_account, -cashFlow, None, None, None, None),
                    ],
                ))
                continue
            if category == "Deposit":
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "",
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                    ],
                ))
                continue
            if category == "Dividend":
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    self.securities[isin][0],
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                        data.Posting(self.pnl_account, -cashFlow, None, None, None, None),
                    ],
                ))
                continue
            if category == "Transfer":
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "",
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                    ],
                ))
                continue

            # This is a trade: buy or sell
            isin = row["ISIN"].strip()
            security = self.securities[isin][0]
            currency = self.securities[isin][1]
            shares = amount.Amount(D(row["shares"].strip()), security)
            sec_account = self.parent_account + ":" + security

            if currency == 'CHF':
                fxRate = None
                # cost = position.CostSpec(D(-cashFlow[0]/shares[0]), D(-cashFlow[0]), currency, None, None, None)
                # cost = position.CostSpec(D(-cashFlow[0] / shares[0]), None, currency, None, None, None)
                cost = position.Cost(D(-cashFlow[0] / shares[0]), currency, None, None)
            else:
                fxRate = amount.Amount(fetch_cached_price(Source(), '{}-CHF'.format(currency), book_date)[0], currency)
                number_total = D(-cashFlow[0] * fxRate[0])
                number_per = D(number_total/shares[0])
                # cost = position.CostSpec(number_per, number_total, currency, None, None, None)
                # cost = position.CostSpec(number_per, None, currency, None, None, None)
                cost = position.Cost(number_per, currency, None, None)

            # Buy
            if category == "Buy":
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "",
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, fxRate, None, None),
                        data.Posting(sec_account, shares, cost, None, None, None)
                    ],
                ))
                continue

            # Sell
            if category == "Sell":
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "",
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    build_sell_postings(
                        entries,
                        sec_account,
                        self.parent_account + ':Cash',
                        self.pnl_account,
                        book_date,
                        shares,
                        cashFlow,
                        fxRate,
                        currency
                    ),
                ))
                continue

            # Should never reach here
            raise Warning('Unknown category {}'.format(category))

        return entries



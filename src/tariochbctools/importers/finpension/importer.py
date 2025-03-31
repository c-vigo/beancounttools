import csv
import datetime
from io import StringIO
from typing import List, Dict
import copy
import logging

from beancount.core import amount, data, position
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from dateutil.parser import parse


def build_sell_postings(
        entries,
        sec_account: str,
        cash_account: str,
        pnl_account: str,
        lot_date: datetime.date,
        shares: amount.Amount,
        price: amount.Amount,
        proceeds: amount.Amount,
        fx_rate: amount.Amount,
        currency: str
) -> List[data.Posting]:
    buys: List[Dict] = []
    sells: List[Dict] = []
    for entry in entries:
        # It is a transaction
        if not isinstance(entry, data.Transaction):
            continue
        entry = entry

        # Up to given date
        if entry.date > lot_date:
            continue

        # Find this account
        for posting in entry.postings:
            if posting.account == sec_account:
                # Buy or sell?
                if posting.units[0] > 0:
                    buys.append({
                        'units': posting.units,
                        'cost': posting.cost,
                        'date': entry.date
                    })
                else:
                    sells.append({
                        'units': posting.units,
                        'cost': posting.cost,
                        'date': entry.date
                    })

    # Sort and process sales
    buys.sort(key=lambda x: x.get('date'))
    sells.sort(key=lambda x: x.get('date'))
    inventory = buys
    for sell in sells:
        inventory, _ = sell_from_lot(inventory, sell)

    # Sell lot
    inventory, sold_lots = sell_from_lot(
        inventory,
        {
            'units': shares,
            'cost': None,
            'date': lot_date
        })

    # Calculate pnl
    pnl_cash_flow = -proceeds[0]
    share_currency = 'CHF'
    if fx_rate is not None:
        pnl_cash_flow = -proceeds[0]*fx_rate[0]
        share_currency = fx_rate[1]
        price = amount.Amount(D(price[0]*fx_rate[0]), fx_rate[1])
    for lot in sold_lots:
        pnl_cash_flow += D(lot['cost'][0] * lot['units'][0])

    # Build postings
    postings = [
        data.Posting(cash_account, proceeds, None, fx_rate, None, None),
        data.Posting(pnl_account, amount.Amount(D(pnl_cash_flow), share_currency), None, None, None, None)
    ]

    for lot in sold_lots:
        postings.append(data.Posting(sec_account, -lot['units'], lot['cost'], price, None, None))

    return postings

def sell_from_lot(inventory: List[Dict], sell_lot: Dict) -> tuple[List[Dict], List[Dict]]:
    target_sell = sell_lot
    security = sell_lot['units'][1]

    # FIFO selling
    sold_lots = []
    for index, lot in enumerate(copy.deepcopy(inventory)):
        # Difference between shares to be sold and shares in this lot
        leftover = lot['units'][0] + sell_lot['units'][0]

        # Exact units to cover the remaining units
        if leftover == D(0):
            # Add the entire lot to "sold lots"
            sold_lots += [lot]

            # Remove the lot from the inventory
            inventory[index] = None

            # Break signal
            sell_lot = None
            break

        # More than enough units to cover the remaining units
        if leftover > 0:
            # Remaining units in this lot
            inventory[index]['units'] = data.Amount(leftover, security)

            # Sold units
            lot['units'] = data.Amount(-sell_lot['units'][0], security)
            sold_lots += [lot]

            # Break signal
            sell_lot = None
            break

        # Consume this lot and continue to the next one
        else:
            # Remove the lot from the inventory
            inventory[index] = None

            # Sold units
            sold_lots += [lot]

            # Reduce the target lot
            sell_lot['units'] = data.Amount(sell_lot['units'][0] + lot['units'][0], security)

    # Successful sale?
    if sell_lot is not None:
        logging.warning('Error selling {} from {}\nSold: {}'.format(target_sell, inventory, sold_lots))

    return list(filter(None, inventory)), sold_lots


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for FinPension CSV files."""

    def __init__(self, regexps, parent_account, income_account, fees_account, securities):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.parent_account = parent_account
        self.income_account = income_account
        self.fees_account = fees_account
        self.securities = securities

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

            # Fees, Deposits & Dividends
            if category == "Flat-rate administrative fee":
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "FinPension",
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                        data.Posting(self.fees_account, -cashFlow, None, None, None, None),
                    ],
                ))
            elif category == "Deposit":
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "FinPension",
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                    ],
                ))
            elif category == "Interests":
                interests_account = '{}:Interests'.format(self.income_account)
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "FinPension",
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                        data.Posting(interests_account, -cashFlow, None, None, None, None),
                    ],
                ))
            elif category == "Dividend":
                security = self.securities[row["ISIN"].strip()][0]
                pnl_account = '{}:{}:Dividends'.format(self.income_account, security)
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "FinPension",
                    'Dividends {}'.format(security),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                        data.Posting(pnl_account, -cashFlow, None, None, None, None),
                    ],
                ))
            elif category == "Transfer":
                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "FinPension",
                    category,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                    ],
                ))
            elif category in ['Buy', 'Sell']:
                # This is a trade: buy or sell
                isin = row["ISIN"].strip()
                security = self.securities[isin][0]
                shares = amount.Amount(D(row["shares"].strip()), security)
                sec_account = self.parent_account + ":" + security
                cost_spec = position.CostSpec(None, -cashFlow[0], 'CHF', None, None, None)

                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "FinPension",
                    '{} {}'.format(category, security),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.parent_account + ':Cash', cashFlow, None, None, None, None),
                        data.Posting(sec_account, shares, cost_spec, None, None, None)
                    ],
                ))
            else:
                raise Warning('Unknown category {}'.format(category))
        return entries

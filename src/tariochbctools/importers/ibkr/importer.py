import logging
import datetime
import re
from datetime import date
from decimal import Decimal
from os import path
from typing import List, Dict
from collections import OrderedDict
import copy
import itertools

import yaml
from beancount.core import amount, data, position
from beancount.core.number import D
from beancount.ingest import importer
from beancount.parser import options
from beancount.query import query
from beancount.ingest.importers.mixins import identifier
from ibflex import Types, client, parser
from ibflex.enums import CashAction
from csv import DictReader
from dateutil.parser import parse

from tariochbctools.importers.general.priceLookup import PriceLookup

cash_commodities = [
    'CHF',
    'USD',
    'EUR'
]


class Importer(importer.ImporterProtocol):
    """An importer for Interactive Broker using the flex query service."""

    def identify(self, file):
        return path.basename(file.name).endswith("ibkr.yaml")

    def file_account(self, file):
        return ""

    def matches(self, trx, t, account):
        p = re.compile(r".* (?P<perShare>\d+\.?\d+) PER SHARE")

        trxPerShareGroups = p.search(trx.description)
        tPerShareGroups = p.search(t["description"])

        trxPerShare = trxPerShareGroups.group("perShare") if trxPerShareGroups else ""
        tPerShare = tPerShareGroups.group("perShare") if tPerShareGroups else ""

        return (
            t["date"] == trx.dateTime
            and t["symbol"] == trx.symbol
            and trxPerShare == tPerShare
            and t["account"] == account
        )

    def extract(self, file, existing_entries):
        with open(file.name, "r") as f:
            config = yaml.safe_load(f)
        token = config["token"]
        queryId = config["queryId"]

        priceLookup = PriceLookup(existing_entries, config["baseCcy"])

        response = client.download(token, queryId)
        statement = parser.parse(response)
        assert isinstance(statement, Types.FlexQueryResponse)

        result = []
        for stmt in statement.FlexStatements:
            transactions = []
            account = stmt.accountId
            for trx in stmt.Trades:
                result.append(
                    self.createBuy(
                        trx.tradeDate,
                        account,
                        trx.symbol.rstrip("z"),
                        trx.quantity,
                        trx.currency,
                        trx.tradePrice,
                        amount.Amount(
                            round(-trx.ibCommission, 2), trx.ibCommissionCurrency
                        ),
                        amount.Amount(round(trx.netCash, 2), trx.currency),
                        config["baseCcy"],
                        trx.fxRateToBase,
                    )
                )

            for trx in stmt.CashTransactions:
                existingEntry = None
                if CashAction.DIVIDEND == trx.type or CashAction.WHTAX == trx.type:
                    existingEntry = next(
                        (
                            t
                            for t in transactions
                            if self.matches(trx, t, stmt.accountId)
                        ),
                        None,
                    )

                if existingEntry:
                    if CashAction.WHTAX == trx.type:
                        existingEntry["whAmount"] += trx.amount
                    else:
                        existingEntry["amount"] += trx.amount
                        existingEntry["description"] = trx.description
                        existingEntry["type"] = trx.type
                else:
                    if CashAction.WHTAX == trx.type:
                        amt = 0
                        whAmount = trx.amount
                    else:
                        amt = trx.amount
                        whAmount = 0

                    transactions.append(
                        {
                            "date": trx.dateTime,
                            "symbol": trx.symbol,
                            "currency": trx.currency,
                            "amount": amt,
                            "whAmount": whAmount,
                            "description": trx.description,
                            "type": trx.type,
                            "account": account,
                        }
                    )

            for trx in transactions:
                if trx["type"] == CashAction.DIVIDEND:
                    asset = trx["symbol"].rstrip("z")
                    payDate = trx["date"].date()
                    totalDividend = trx["amount"]
                    totalWithholding = -trx["whAmount"]
                    totalPayout = totalDividend - totalWithholding
                    currency = trx["currency"]
                    account = trx["account"]

                    result.append(
                        self.createDividen(
                            totalPayout,
                            totalWithholding,
                            asset,
                            currency,
                            payDate,
                            priceLookup,
                            trx["description"],
                            account,
                        )
                    )

        return result

    def createDividen(
        self,
        payout: Decimal,
        withholding: Decimal,
        asset: str,
        currency: str,
        date: date,
        priceLookup: PriceLookup,
        description: str,
        account: str,
    ):
        narration = "Dividend: " + description
        liquidityAccount = self.getLiquidityAccount(account, currency)
        incomeAccount = self.getIncomeAccount(account)
        assetAccount = self.getAssetAccount(account, asset)

        price = priceLookup.fetchPrice(currency, date)

        postings = [
            data.Posting(
                assetAccount, amount.Amount(D(0), asset), None, None, None, None
            ),
            data.Posting(
                liquidityAccount,
                amount.Amount(payout, currency),
                None,
                price,
                None,
                None,
            ),
        ]
        if withholding > 0:
            receivableAccount = self.getReceivableAccount(account)
            postings.append(
                data.Posting(
                    receivableAccount,
                    amount.Amount(withholding, currency),
                    None,
                    None,
                    None,
                    None,
                )
            )
        postings.append(data.Posting(incomeAccount, None, None, None, None, None))

        meta = data.new_metadata("dividend", 0, {"account": account})
        return data.Transaction(
            meta, date, "*", "", narration, data.EMPTY_SET, data.EMPTY_SET, postings
        )

    def createBuy(
        self,
        date: date,
        account: str,
        asset: str,
        quantity: Decimal,
        currency: str,
        price: Decimal,
        commission: amount.Amount,
        netCash: amount.Amount,
        baseCcy: str,
        fxRateToBase: Decimal,
    ):
        narration = "Buy"
        feeAccount = self.getFeeAccount(account)
        liquidityAccount = self.getLiquidityAccount(account, currency)
        assetAccount = self.getAssetAccount(account, asset)

        liquidityPrice = None
        if currency != baseCcy:
            price = price * fxRateToBase
            commission = amount.Amount(
                round(commission.number * fxRateToBase, 2), baseCcy
            )
            liquidityPrice = amount.Amount(fxRateToBase, baseCcy)

        postings = [
            data.Posting(
                assetAccount,
                amount.Amount(quantity, asset),
                data.Cost(price, baseCcy, None, None),
                None,
                None,
                None,
            ),
            data.Posting(feeAccount, commission, None, None, None, None),
            data.Posting(
                liquidityAccount,
                netCash,
                None,
                liquidityPrice,
                None,
                None,
            ),
        ]

        meta = data.new_metadata("buy", 0, {"account": account})
        return data.Transaction(
            meta, date, "*", "", narration, data.EMPTY_SET, data.EMPTY_SET, postings
        )

    def getAssetAccount(self, account: str, asset: str):
        return f"Assets:{account}:Investment:IB:{asset}"

    def getLiquidityAccount(self, account: str, currency: str):
        return f"Assets:{account}:Liquidity:IB:{currency}"

    def getReceivableAccount(self, account: str):
        return f"Assets:{account}:Receivable:Verrechnungssteuer"

    def getIncomeAccount(self, account: str):
        return f"Income:{account}:Interest"

    def getFeeAccount(self, account: str):
        return f"Expenses:{account}:Fees"


class CsvImporter(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Interactive Brokers Flex Query CSV files."""

    def __init__(self, regexps, parent_account, income_account, tax_account, fees_account):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.parent_account = parent_account
        self.income_account = income_account
        self.cash_account = parent_account + ':Cash'
        self.interests_account = income_account + ':Interests'
        self.tax_account = tax_account
        self.fees_account = fees_account

    def name(self):
        return super().name() + self.parent_account

    def file_account(self, file):
        return self.parent_account

    def extract(self, file, existing_entries=None):
        entries = []
        withholding_taxes = []

        with open(file.name, 'r', encoding='utf8') as csvfile:
            reader = DictReader(
                csvfile,
                fieldnames=[
                    "Id",
                    "Date",
                    "Type",
                    "Currency",
                    "Proceeds",
                    "Security",
                    "Amount",
                    "CostBasis",
                    "TradePrice",
                    "Commission",
                    "CommissionCurrency",
                ],
                delimiter=","
            )
            for row in reader:
                # Parse
                category = row['Type']
                book_date = parse(row["Date"].strip()).date()
                meta = data.new_metadata(file.name, reader.line_num)
                meta['document'] = '{}-12-31-InteractiveBrokers_ActivityReport.pdf'.format(book_date.year)
                meta['trans_id'] = row['Id']
                cashFlow = amount.Amount(D(row["Proceeds"]), row["Currency"])
                security = row["Security"]

                # Deposits and withdrawals
                if category == 'Deposits/Withdrawals':
                    entries.append(data.Transaction(
                        meta,
                        book_date,
                        "*",
                        "Interactive Brokers",
                        "Deposit" if cashFlow[0] > 0 else "Withdrawal",
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        [data.Posting(self.cash_account, cashFlow, None, None, None, None)],
                    ))

                # Dividends
                elif category == 'Dividends':
                    dividend_account = self.income_account + ':' + security + ':Dividends'
                    entries.append(data.Transaction(
                        meta,
                        book_date,
                        "*",
                        "Interactive Brokers",
                        "Dividends {}".format(security),
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        [
                            data.Posting(self.cash_account, cashFlow, None, None, None, None),
                            data.Posting(dividend_account, -cashFlow, None, None, None, None)
                        ],
                    ))

                # Withholding tax
                elif category == 'Withholding Tax':
                    withholding_taxes.append([
                        security,
                        book_date,
                        cashFlow,
                        False,
                        meta
                    ])

                # Interests
                elif category == 'Broker Interest Received':
                    entries.append(data.Transaction(
                        meta,
                        book_date,
                        "*",
                        "Interactive Brokers",
                        "Interests",
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        [
                            data.Posting(self.cash_account, cashFlow, None, None, None, None),
                            data.Posting(self.interests_account, -cashFlow, None, None, None, None)
                        ],
                    ))

                # FX Exchange
                elif category in ['BUY', 'SELL'] and '.' in security:
                    commission = amount.Amount(D(row["Commission"]), row["CommissionCurrency"])
                    fx_orig = amount.Amount(D(row["Amount"]), row["Security"][:3])
                    fx_dest = amount.Amount(D(row["Proceeds"]), row["Security"][4:])
                    fx_rate = amount.Amount(D(row["TradePrice"]), row["Security"][4:])

                    postings = [
                        data.Posting(self.cash_account, fx_orig, None, fx_rate, None, None),
                        data.Posting(self.cash_account, fx_dest, None, None, None, None)
                    ]

                    if commission[0] != 0:
                        postings.append(data.Posting(self.cash_account, commission, None, None, None, None))
                        postings.append(data.Posting(self.fees_account, -commission, None, None, None, None))

                    entries.append(data.Transaction(
                        meta,
                        book_date,
                        "*",
                        "Interactive Brokers",
                        "FX Exchange {}".format(row["Security"]),
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        postings
                    ))

                # Trade: buy
                elif category == 'BUY':
                    # Parse more fields
                    commission = amount.Amount(D(row["Commission"]), row["CommissionCurrency"])
                    shares = amount.Amount(D(row["Amount"]), security)
                    cost_per_share = position.Cost(D(row["TradePrice"]), row["Currency"], book_date, None)
                    proceeds = amount.Amount(D(row["Proceeds"]) + D(row["Commission"]), row["Currency"])
                    security_account = self.parent_account + ':' + security

                    postings = [
                            data.Posting(self.cash_account, proceeds, None, None, None, None),
                            data.Posting(security_account, shares, cost_per_share, None, None, None),
                            data.Posting(self.fees_account, -commission, None, None, None, None)
                    ]

                    entries.append(data.Transaction(
                        meta,
                        book_date,
                        "*",
                        "Interactive Brokers",
                        "Buy {}".format(row["Security"]),
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        postings
                    ))

                # Trade: sell
                elif category == 'SELL':
                    shares = amount.Amount(D(row["Amount"]), security)
                    price = amount.Amount(D(row["TradePrice"]), row["Currency"])
                    commission = amount.Amount(D(row["Commission"]), row["CommissionCurrency"])

                    entries.append(data.Transaction(
                        meta,
                        book_date,
                        "*",
                        "Interactive Brokers",
                        "Sell {}".format(row["Security"]),
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        self.build_fifo_postings(
                            existing_entries + entries,
                            meta['trans_id'],
                            book_date,
                            shares,
                            cashFlow,
                            price,
                            commission
                        ),
                    ))

                # Unrecognized transaction
                else:
                    logging.warning(
                        'File {}: unsupported transaction of type {} on {}'.format(
                            file.name,
                            category,
                            row['Date']
                    ))

        # Append withholding taxes
        for index, entry in enumerate(entries):
            # It is a transaction
            if not isinstance(entry, data.Transaction):
                continue
            entry: data.Transaction = entry

            # It is a dividend transaction
            if "Dividends" not in entry.narration:
                continue

            # Get date and security
            date = entry.date
            security = entry.narration.replace('Dividends ', '')

            # Find withholding tax
            for index2, tax in enumerate(withholding_taxes):
                # Match
                if tax[0] != security or tax[1] != date:
                    continue

                # Double processing?
                if tax[3]:
                    raise Warning('Double match withholding tax for {} on {}'.format(security, date))
                else:
                    withholding_taxes[index2][3] = True

                # Build new postings
                total_cash_flow = data.Amount(D(tax[2][0] + entry.postings[0].units[0]), tax[2][1])
                entries[index] = data.Transaction(
                    entry.meta,
                    entry.date,
                    entry.flag,
                    entry.payee,
                    entry.narration,
                    entry.tags,
                    entry.links,
                    [
                        data.Posting(self.cash_account, total_cash_flow, None, None, None, None),
                        entry.postings[1],
                        data.Posting(self.tax_account, -tax[2], None, None, None, None)
                    ],
                )
                break

            else:  # Withholding tax not found
                raise Warning('Missing withholding tax for {} on {}'.format(security, date))

        # All withholding taxes processed?
        unmatched_withholding_taxes = []
        for tax in withholding_taxes:
            if not tax[3]:
                unmatched_withholding_taxes.append(tax)
        
        # Withholding tax re-calculations
        indexes = []
        for index, tax in enumerate(unmatched_withholding_taxes):
            # Check for tax re-imbursement
            if tax[2][0] > 0:
                # Find tax re-calculations on same date
                for index2, match_tax in enumerate(unmatched_withholding_taxes):
                    if match_tax[0:1] == tax[0:1] and match_tax[2][0] < 0:
                        # Find original dividend
                        for entry in itertools.chain(entries, existing_entries):
                            # It is a transaction
                            if not isinstance(entry, data.Transaction):
                                continue
                            entry: data.Transaction = entry

                            # It is a dividend transaction
                            if "Dividends" not in entry.narration:
                                continue

                            # It is the same security
                            security = entry.narration.replace('Dividends ', '')
                            if security != tax[0]:
                                continue
                            dividend_account = self.income_account + ':' + security + ':Dividends'

                            # It is the same value, opposite sign
                            value = None
                            for posting in entry.postings:
                                if posting.account == self.tax_account:
                                    value = posting.units
                            if value != tax[2]:
                                continue

                            # We got a match! Get date of original dividend payout
                            date = entry.date
                            indexes.append(index)
                            indexes.append(index2)
                            cash_balance = amount.Amount(tax[2][0] + match_tax[2][0], tax[2][1])

                            entries.append(data.Transaction(
                                tax[4],
                                tax[1],
                                "*",
                                "Interactive Brokers",
                                "Dividends {}".format(security),
                                data.EMPTY_SET,
                                data.EMPTY_SET,
                                [
                                    data.Posting(self.cash_account, cash_balance, None, None, None, None),
                                    data.Posting(dividend_account, amount.Amount(D(0), tax[2][1]), None, None, None, None),
                                    data.Posting(self.tax_account, -cash_balance, None, None, None, {'effective_date': '{}'.format(date)})
                                ],
                            ))

        # Unmatched withholding taxes?
        for index, tax in enumerate(unmatched_withholding_taxes):
            if index not in indexes:
                logging.warning('Unmatched withholding tax for {} on {}, value {}'.format(tax[0], tax[1], tax[2]))


        return entries

    def build_fifo_postings(
            self,
            entries,
            transaction_id,
            lot_date: datetime.date,
            shares: amount.Amount,
            proceeds: amount.Amount,
            price: amount.Amount,
            commission: amount.Amount
    ) -> List[data.Posting]:
        # Accounts
        security = shares[1]
        security_account = self.parent_account + ':' + security
        pnl_account = self.income_account + ':' + security + ':PnL'

        # Build inventory
        processed_transactions: List = []
        buys: List[Dict] = []
        sells: List[Dict] = []
        for entry in entries:
            # It is a transaction
            if not isinstance(entry, data.Transaction):
                continue
            entry: data.Transaction = entry

            if 'trans_id' not in entry.meta:
                continue
            trans_id = entry.meta['trans_id']

            # Up to given transaction
            if trans_id >= transaction_id:
                continue

            # Avoid duplicate processing
            if trans_id in processed_transactions:
                continue
            processed_transactions.append(trans_id)

            # Find a trade with this commodity
            for posting in entry.postings:
                if posting.account == security_account:
                    # Buy or sell?
                    if posting.units[0] > 0:
                        buys.append({
                            'id': trans_id,
                            'units': posting.units,
                            'cost': posting.cost,
                            'date': entry.date
                        })
                    else:
                        sells.append({
                            'id': trans_id,
                            'units': posting.units,
                            'cost': posting.cost,
                            'date': entry.date
                        })

        # Sort and process sales
        buys.sort(key=lambda x: x.get('date'))
        sells.sort(key=lambda x: x.get('date'))
        inventory = buys
        for sell in sells:
            inventory, _ = self.sell_from_lot(inventory, sell)

        # Sell lot
        inventory, sold_lots = self.sell_from_lot(
            inventory,
            {
                'id': transaction_id,
                'units': shares,
                'cost': None,
                'date': lot_date
            })

        # Calculate pnl
        pnl_cash_flow = -proceeds[0]
        for lot in sold_lots:
            pnl_cash_flow += D(lot['cost'][0] * lot['units'][0])

        # Build postings
        totalProceeds = data.Amount(D(proceeds[0] + commission[0]), proceeds[1])
        postings = [
            data.Posting(self.cash_account, totalProceeds, None, None, None, None),
            data.Posting(pnl_account, amount.Amount(D(pnl_cash_flow), proceeds[1]), None, None, None, None)
        ]
        if commission[0] != 0:
            postings.append(data.Posting(self.fees_account, -commission, None, None, None, None))

        for lot in sold_lots:
            postings.append(data.Posting(security_account, -lot['units'], lot['cost'], price, None, None))

        return postings

    def sell_from_lot(self, inventory: List[Dict], sell_lot: Dict) -> (List[Dict], List[Dict]):
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

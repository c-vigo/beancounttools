import re

from tariochbctools.importers.general import mt940importer


class ZkbImporter(mt940importer.Importer):
    def prepare_payee(self, trxdata):
        return ""

    def prepare_narration(self, trxdata):
        extra = trxdata["extra_details"]
        details = trxdata["transaction_details"]

        extraReplacements = {}
        extraReplacements[r"Einkauf ZKB Maestro[- ]Karte"] = ""
        extraReplacements[r"LSV:.*"] = "LSV"
        extraReplacements[r"Gutschrift:.*"] = "Gutschrift"
        extraReplacements[r"eBanking:.*"] = "eBanking"
        extraReplacements[r"eBanking Mobile:.*"] = "eBanking Mobile"
        extraReplacements[r"E-Rechnung:.*"] = "E-Rechnung"
        extraReplacements[r"Kontouebertrag:.*"] = "Kontouebertrag:"
        extraReplacements[r"\?ZKB:\d+ "] = ""

        detailsReplacements = {}
        detailsReplacements[r"\?ZI:\?9:\d"] = ""
        detailsReplacements[r"\?ZKB:\d+"] = ""
        detailsReplacements[r"Einkauf ZKB Maestro[- ]Karte Nr. \d+,"] = "Maestro"

        for pattern, replacement in extraReplacements.items():
            extra = re.sub(pattern, replacement, extra)

        for pattern, replacement in detailsReplacements.items():
            details = re.sub(pattern, replacement, details)

        if extra:
            narration = extra.strip() + ": " + details.strip()
        else:
            narration = details.strip()

        return narration


import csv

from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
from datetime import datetime



class ZkbCSVImporter(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for ZKB CSV files."""

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
                    "Date",
                    "Booking text",
                    "Curr",
                    "Amount details",
                    "ZKB reference",
                    "Reference number",
                    "Debit CHF",
                    "Credit CHF",
                    "Value date",
                    "Balance CHF",
                    "Payment purpose",
                    "Details"
                ],
                delimiter=";"
            )
            rows = list(reader)[1:]

        for index, row in enumerate(rows):
            try:
                # Parse transaction
                meta = data.new_metadata(file.name, index)
                meta['zkb_reference'] = row['ZKB reference']

                # Parse date with format DD.MM.YYYY
                book_date = datetime.strptime(row['Date'].strip(), "%d.%m.%Y").date()
                description = row["Booking text"].strip() if row["Booking text"] else ""
                
                currency = row["Curr"].strip() if row["Curr"] else "CHF"
                if row["Debit CHF"] != "":
                    cash_flow = amount.Amount(-D(row["Debit CHF"]), currency)
                else:
                    cash_flow = amount.Amount(D(row["Credit CHF"]), currency)

                entries.append(data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "",
                    description,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self.account, cash_flow, None, None, None, None),
                    ],
                ))

            except BaseException as e:
                raise Warning('Error parsing line {}\n{} from file {}'.format(row, e, file.name))

        return entries

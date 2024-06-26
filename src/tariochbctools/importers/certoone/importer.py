import re
from datetime import datetime, timedelta
from dateutil.parser import parse
from pathlib import Path
import csv
# import matplotlib
from pypdf import PdfReader

import camelot
from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier


def cleanDecimal(formatted_number):
    return D(formatted_number.replace("'", ""))


def parse_pdf_to_csv(pdf_file_name, csv_file_name):
    transactions = []

    # get number of pages
    reader = PdfReader(pdf_file_name)
    n_pages = len(reader.pages)

    # Read tables
    try:
        tables = camelot.read_pdf(
            pdf_file_name, pages='2-{}'.format(n_pages-2), flavor="stream", table_areas=["50,700,560,90"]
        )
    except ValueError:
        tables = camelot.read_pdf(
            pdf_file_name, pages='2-{}'.format(n_pages-3), flavor="stream", table_areas=["50,700,560,90"]
        )

    # Visual debugging
    # camelot.plot(tables[0], kind='text').show()
    # camelot.plot(tables[0], kind='grid').show()

    balance_date = None
    balance_amount = None

    for table in tables:
        for index, row in table.df.iterrows():
            if len(tuple(row)) == 5:
                _, book_date, text, credit, debit = tuple(row)
            elif len(tuple(row)) == 4:
                book_date, text, credit, debit = tuple(row)
            else:
                # Balance in a separate table
                text, value = tuple(row)
                balance_date = re.search(
                    r"Saldo per (\d\d\.\d\d\.\d\d\d\d) zu unseren Gunsten CHF", text
                ).group(1)
                balance_date = datetime.strptime(balance_date, "%d.%m.%Y").date()
                # add 1 day: cembra provides balance at EOD, but beancount checks it at SOD
                balance_date = balance_date + timedelta(days=1)
                balance_amount = cleanDecimal(value)
                continue

            book_date, text, credit, debit = (
                book_date.strip(),
                text.strip(),
                credit.strip(),
                debit.strip(),
            )

            # Transaction entry
            try:
                book_date = datetime.strptime(book_date, "%d.%m.%Y").date()
            except Exception:
                book_date = None

            if book_date:
                value = - cleanDecimal(debit) if debit else cleanDecimal(credit)
                transactions.append([book_date, value, text])
                continue

            # Balance entry
            try:
                balance_date = re.search(
                    r"Saldo per (\d\d\.\d\d\.\d\d\d\d) zu unseren Gunsten CHF", text
                ).group(1)
                balance_date = datetime.strptime(balance_date, "%d.%m.%Y").date()
                # add 1 day: cembra provides balance at EOD, but beancount checks it at SOD
                balance_date = balance_date + timedelta(days=1)
                balance_amount = cleanDecimal(debit) if debit else - cleanDecimal(credit)
            except Exception:
                pass


    # Write to CSV file
    with open(csv_file_name, 'wt') as f:
        # Header
        f.write('Date;Amount;Description\n')

        # Balance
        if balance_date is not None and balance_amount is not None:
            f.write('{};{};BALANCE\n'.format(balance_date, balance_amount))

        # Transactions
        for transaction in transactions:
            f.write('{};{};{}\n'.format(*transaction))


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Cembra Certo One Statement PDF files."""

    def __init__(self, regexps, account):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.currency = "CHF"

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        entries = []

        # Parse the PDF to a CSV file
        csv_file = Path(file.name).with_suffix('.csv')
        if not csv_file.is_file():
            parse_pdf_to_csv(file.name, str(csv_file))

        # Read the CSV file
        with open(str(csv_file), 'r') as csvfile:
            reader = csv.reader(
                csvfile,
                delimiter=";"
            )
            rows = list(reader)

        # Balance
        entries.append(data.Balance(
            data.new_metadata(file.name, 0),
            parse(rows[1][0].strip(), dayfirst=False).date(),
            self.account,
            amount.Amount(-D(rows[1][1]), self.currency),
            None,
            None
        ))

        # Transactions
        for row in rows[2:]:
            date = parse(row[0].strip(), dayfirst=False).date()
            cash_flow = D(row[1])
            desc = row[2]
            meta = data.new_metadata(file.name, 0)
            # meta['document'] = Path(file.name).name
            entries.append(data.Transaction(
                meta,
                date,
                "*",
                "",
                desc,
                data.EMPTY_SET,
                data.EMPTY_SET,
                [data.Posting(self.account, amount.Amount(D(cash_flow), self.currency), None, None, None, None)],
            ))

        return entries

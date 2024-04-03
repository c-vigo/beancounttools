from pathlib import Path

import camelot
from pypdf import PdfReader
from itertools import chain
from datetime import datetime
import csv

from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier


def parse_pdf_to_csv(pdf_file_name, csv_file_name):
    # get number of pages
    reader = PdfReader(pdf_file_name)
    n_pages = len(reader.pages)

    # read the different pages
    first_table = camelot.read_pdf(
        pdf_file_name,
        pages='1',
        flavor="stream",
        table_areas=["0,600,590,40"],
        columns=['470']
    )

    table_areas = ["0,700,590,60"]
    # Case 1: one pages
    if n_pages == 1:
            tables = first_table
    # Case 2: two pages
    elif n_pages == 2:
        try:
            last_table = camelot.read_pdf(
                pdf_file_name,
                pages='2',
                flavor="stream",
                table_areas=table_areas,
                columns=['470']
            )
            tables = chain(first_table, last_table)
        except ValueError:
            tables = first_table
    # Case 3: more than two pages
    else:
        try:
            mid_tables = camelot.read_pdf(
                pdf_file_name,
                pages='2-{}'.format(n_pages-1),
                flavor="stream",
                table_areas=table_areas,
                columns=['470']
            )
            last_table = camelot.read_pdf(
                pdf_file_name,
                pages='end',
                flavor="stream",
                table_areas=table_areas,
                columns=['470']
            )
            tables = chain(first_table, mid_tables, last_table)
        except ValueError:
            mid_tables = camelot.read_pdf(
                pdf_file_name,
                pages='2-{}'.format(n_pages-2),
                flavor="stream",
                table_areas=table_areas,
                columns=['470']
            )
            last_table = camelot.read_pdf(
                pdf_file_name,
                pages='{}'.format(n_pages-1),
                flavor="stream",
                table_areas=table_areas,
                columns=['470']
            )
            tables = chain(first_table, mid_tables, last_table)

    tables = list(tables)

    # Process tables into individual orders
    orders = []
    for index, table in enumerate(tables):
        table = table.df.values.tolist()
        if index == len(tables)-1:
            n_rows = len(table) - 1
        else:
            n_rows = len(table)
            
        assert n_rows % 4 == 0
        orders.extend([table[i:i+4] for i in range(0, n_rows-1, 4)])

    orders = [[item for sublist in order for item in sublist] for order in orders]

    # Process orders into transactions
    transactions = []
    special_char_map = {ord('ä'):'ae', ord('ü'):'ue', ord('ö'):'oe', ord('ß'):'ss'}

    for order in orders:
        # Parse fields
        description = order[0].translate(special_char_map)
        cost = amount.Amount(D(order[1].split()[0]), order[1].split()[1])
        traveller = order[2].replace('Traveller: ', '')
        travel_date = datetime.strptime(order[3], "Travel date: %d.%m.%Y").date()
        delivery_address = order[4].replace('Delivery address: ', '')
        order_date = datetime.strptime(order[5], "Order date: %d.%m.%Y").date()
        order_num = int(order[7].replace('Order no.: ', ''))

        transactions.append([
            order_date,
            travel_date,
            description,
            cost[0],
            cost[1],
            traveller,
            delivery_address,
            order_num
            ])
    
    # Write to CSV file
    with open(csv_file_name, 'wt') as f:
        # Header
        f.write('Order Date; Travel Date; Description; Value; Currency; Traveller; Delivery Address; Order Number\n')

        # Transactions
        for transaction in transactions:
            f.write('{};{};{};{};{};{};{};{}\n'.format(*transaction))


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for SBB Order Summary PDF files."""

    def __init__(self, regexps, account):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
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

        # Transactions
        return [
            data.Transaction(
                data.new_metadata(
                    filename=file.name,
                    lineno=line_number,
                    kvlist=dict({
                        'orderno': row[7],
                        'traveller': row[5],
                        'email': row[6],
                        'travel_date': row[1]
                    })),
                datetime.strptime(row[0], "%Y-%m-%d").date(),
                "*",
                "SBB",
                row[2],
                data.EMPTY_SET,
                data.EMPTY_SET,
                [data.Posting(self.account, amount.Amount(-D(row[3]), row[4]), None, None, None, None)],
            )
            for line_number, row in enumerate(rows[1:])
        ]

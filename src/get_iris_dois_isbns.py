import re

from src.read_iris import read_iris


def get_iris_dois_isbns():
    df_filtered = read_iris()

    dois = df_filtered.select('IDE_DOI').drop_nulls().unique()['IDE_DOI'].to_list()

    #filter and normalize the dois
    doi_rule = re.compile(r'10\.\d{4,}\/[^,\s;]*')
    not_doi = []
    filtered_dois = []

    for doi in dois:
        match = doi_rule.search(doi)
        if match:
            filtered_dois.append('doi:' + match.group())
        else:
            not_doi.append(doi)

    isbns = df_filtered.select('IDE_ISBN').drop_nulls().unique()['IDE_ISBN'].to_list()

    #filter and normalize the isbns
    isbn_rule = re.compile(r'(ISBN[-]*(1[03])*[ ]*(: ){0,1})*(([0-9Xx][- ]*){13}|([0-9Xx][- ]*){10})') # ??? results to check
    not_isbn = []
    filtered_isbns = []

    for isbn in isbns:
        if isbn_rule.search(isbn) is not None:
            filtered_isbns.append('isbn:' + isbn.replace('-', '').replace(' ', ''))
        else:
            not_isbn.append(isbn)

    dois_isbns = filtered_dois + filtered_isbns
    dois_isbns = [id.lower() for id in dois_isbns]

    return dois_isbns
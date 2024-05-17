import re

from src.read_iris import read_iris


def get_iris_dois_isbns():
    df_filtered = read_iris()

    #filter and normalize the dois
    dois = df_filtered.select('IDE_DOI').drop_nulls().unique()['IDE_DOI'].to_list()

    doi_rule = re.compile(r'10\.\d{4,}\/[^,\s;]*')
    not_doi = []
    filtered_dois = []

    for doi in dois:
        match = doi_rule.search(doi)
        if match:
            filtered_dois.append('doi:' + match.group())
        else:
            not_doi.append(doi)


    #filter and normalize the isbns
    isbns = df_filtered.select('IDE_ISBN').drop_nulls().unique()['IDE_ISBN'].to_list()

    isbn_rule = re.compile(r'(ISBN[-]*(1[03])*[ ]*(: ){0,1})*(([0-9Xx][- ]*){13}|([0-9Xx][- ]*){10})') # ??? results to check
    not_isbn = []
    filtered_isbns = []

    for isbn in isbns:
        if isbn_rule.search(isbn) is not None:
            filtered_isbns.append('isbn:' + isbn.replace('-', '').replace(' ', ''))
        else:
            not_isbn.append(isbn)


    #filter and normalize the pmids
    pmids = df_filtered.select('IDE_PMID').drop_nulls().unique()['IDE_PMID'].to_list()

    pmid_rule = re.compile(r'(?!0\d{0,7})\d{1,8}')
    not_pmids = []
    filtered_pmids = []

    for pmid in pmids:
        match = pmid_rule.search(pmid)
        if pmid_rule.search(pmid) is not None and 'PMC' not in pmid: # questo da sottolineare in documentazione
            filtered_pmids.append('pmid:' + match.group())
        else:
            not_pmids.append(pmid)


    dois_isbns_pmids = filtered_dois + filtered_isbns + filtered_pmids
    dois_isbns_pmids = [id.lower() for id in dois_isbns_pmids]

    return dois_isbns_pmids
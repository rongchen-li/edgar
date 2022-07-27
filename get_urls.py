#!/apps/anaconda3/bin/python3
"""
This program automatically queries filings from SEC Edgar, and returns a csv of search results.
E-filing on SEC Edgar became mandatory for U.S. public firms since 1994 (https://www.sec.gov/info/edgar/regoverview.htm)
Example: 
    params = {'text': '10-K', 'first': 2018, 'last': 2020}
"""

# %% Packages
import os
import re
import sys
import time
import requests
import pandas as pd
from lxml import etree
from io import StringIO


# Raw string literals to find the matched content within all query results
MATCHER = r'Your search matched <b>(.*?)</b>'
# Edgar search page headers, used as sanity check for HTML parsing
COLUMNS = {'No.': 'no.',
           'Company': 'coname',
           'Format': 'fname',
           'Form Type': 'form',
           'Filing Date': 'fdate',
           'Size': 'fsize'}


# %% Functions
# SEC Edgar search, return a pandas dataframe of search results, with urls to filings of txt formats
def get_urls(keywords, start, end, chunk_size=50):

    # Local function to get the total number of files and txt urls
    def srch_query(params, extra={}):
        # Search for given type(s) of filings on the Edgar engine
        params_extra = params.copy()
        for kk, vv in extra.items():
            params_extra[kk] = vv
        r = requests.get('https://www.sec.gov/cgi-bin/srch-edgar', params=params_extra)
        # Forced delay
        time.sleep(1)
        # Total number of matched results and raw HTML text of the returned page
        # If the returned page is empty, return nothing
        try:
            to_return = (r.text, int(re.findall(MATCHER, r.text)[0]))
            return to_return
        except Exception as exc:
            print('Edgar search failed: {}'.format(exc))
            return '', -1

    # Local lambda function to parse the Edgar search result page
    def srch_parse(idx, elem):
        if idx != 1 and idx != 2:
            return elem.text
        elif idx == 1:
            return elem[0].text
        else:
            return 'https://www.sec.gov{}'.format(elem[0].attrib['href'])

    # First print the total number of matched results
    params = {'text': keywords, 'first': start, 'last': end}
    num = srch_query(params)[1]
    print('Looking for {} results for keywords {} from {} to {}'.format(num, keywords, start, end))
    # Loop through each page
    to_return = []
    for nn in range(0, num, chunk_size):
        # Query again, by page
        extra = {'start': nn + 1, 'count': chunk_size - 1}
        text, num_ = srch_query(params, extra)
        assert num == num_, 'Inconsistency in search results'
        # Parse the HTML table
        tab = etree.parse(StringIO(text), parser=etree.HTMLParser()).find('body/div/table')
        rows = iter(tab)
        assert [cc.text for cc in next(rows)] == list(COLUMNS.keys()), 'Nonconforming headers'
        headers = list(COLUMNS.values())
        for rr in rows:
            values = [srch_parse(jj, cc) for jj, cc in enumerate(rr)]
            to_return.append(dict(zip(headers, values)))
    assert num == len(to_return), 'Errors occurred during parsing'
    to_return = pd.DataFrame(to_return)
    return to_return


# %% Main
if __name__ == '__main__':
    # Take search params from the command line and query the SEC Edgar server
    df_urls = get_urls(keywords=sys.argv[1], start=sys.argv[2], end=sys.argv[3])
    # Store Edgar search results
    if os.path.exists('Catalogs'):
        pass
    else:
        os.mkdir('Catalogs')
    df_urls.to_csv('Catalogs/' + '_'.join(sys.argv[1:]) + '.csv', index=False)
    # Tabulate results
    print('{} unique urls stored: '.format(len(df_urls['fname'].unique())))
    print(df_urls['form'].value_counts())

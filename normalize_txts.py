#!/apps/anaconda3/bin/python3


# %% Packages
import re
import os
import sys
import time
import json
import requests
import unicodedata
import pandas as pd
from bs4 import BeautifulSoup
from restore_windows_1252_characters import *
from concurrent.futures import ThreadPoolExecutor, as_completed


# %% Functions
# Function to requests the raw txt Edgar e-filing and normalize it
def normalize_txt(txt, keep_tab=False): # The include_ex and keep_tab functionality is being worked on

    start_time = time.time()

    # Convert the raw text we have to extracted to BeautifulSoup object
    soup = BeautifulSoup(txt, 'html.parser')
    # Remove all script and style elements
    for scr in soup(['script', 'style']):
        scr.decompose()
    # Remove XBRL tags left in the HTML body (newly added)
    for xb in soup.find_all(re.compile(r'xbrli:')):
        xb.decompose()
    """
    for ix in soup.find_all(re.compile(r'ix:')):
        ix.decompose()
    """
    # Extract all tables, including table headers (Alternatively, keep tables elsewhere?)
    if not keep_tab:
        for tab in soup.find_all('table'):
            tab.extract()
    # Parse out HTML body text
    text = soup.get_text('\n')

    # Upper case to facilitate further cleaning
    text = text.upper()

    # Replace \&NBSP and \&#160 with a blank space; Replace \&AMP and \&#38 with '&'
    text = re.sub('(&#160;|&NBSP;)', ' ', text)
    text = re.sub('(&#38;|&AMP;)', '&', text)

    # Unicode normalization, including line breakers
    text = unicodedata.normalize("NFKD", text)
    text = '\n'.join(text.splitlines())

    # Further utf-8 normalization
    text = restore_windows_1252_characters(text)

    # Take care of line-breakers & whitespaces combinations due to BeautifulSoup parsing
    text = re.sub(r'[ ]+\n', '\n', text)
    text = re.sub(r'\n[ ]+', '\n', text)
    text = re.sub(r'\n+', '\n', text)

    # Move Period to beginning
    text = text.replace('\n.\n', '.\n')

    # Reformat item headers for later extraction
    text = text.replace('\nI\nTEM', '\nITEM')
    text = text.replace('\nITEM\n', '\nITEM ')
    text = text.replace('\nITEM  ', '\nITEM ')
    text = text.replace(':\n', '.\n')

    # Math symbols for clearer looks
    text = text.replace('$\n', '$')
    text = text.replace('\n%', '%')

    # Reformat by additional line-breakers
    text = text.replace('\n', '\n\n')

    end_time = time.time()
    print('Filing finished processing in %.2f seconds.' % (end_time - start_time))

    return text



# Parallel processing wrapper of the core function normalize_txt()
def normalize_txts(corpus, keep_tab=False, max_workers=4):

    to_return = dict.fromkeys(corpus.keys())
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(normalize_txt, txt, keep_tab):\
                             url for url, txt in corpus.items()}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                text = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
            else:
                to_return[url] = text
    end_time = time.time()
    print('%d filings finished processing in %.2f seconds.' % (len(corpus), end_time - start_time))
    return to_return


# %% Main
if __name__ == '__main__':
    # Input the Edgar search results saved from the get_urls()
    in_file = sys.argv[1] + '/' + sys.argv[2]
    with open('Sources/'+in_file, 'r', encoding='utf-8') as f:
        corpus = {k: v['text'] for k,v in json.load(f).items() if v is not None}
    f.close()

    # Request and normalize Edgar filings
    js_txts = normalize_txts(corpus, keep_tab=False) # Make keep_tab kwargs??
    
    # Write to a json file
    out_file = in_file
    if os.path.exists(sys.argv[3]+'/'+sys.argv[1]):
        pass
    else:
        os.mkdir(sys.argv[3]+'/'+sys.argv[1])
    with open(sys.argv[3]+'/'+out_file, 'w', encoding='utf-8') as f:
        json.dump(js_txts, f)
    f.close()

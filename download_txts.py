#!/apps/anaconda3/bin/python3


# %% Packages
import re
import os
import sys
import time
import json
import random
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


'''
Headers used for Python requests, otherwise could be recognized as bots
    ... especially when launching the program on a server)
(?) Randomized in the future -- uniformity?
'''
HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36n(KHTML, like Gecko) '
                         'Chrome/80.0.3987.116 Safari / 537.36'}


# %% Functions
# Function to requests the raw txt Edgar e-filing and normalize it
def download_txt(url, form_type, include_ex=False): # The include_ex functionality is being worked on

    start_time = time.time()

    # Get the HTML data
    r = requests.get(url, headers=HEADERS)
    time.sleep(random.randint(1, 5))

    # Get raw filing (10-K, 10-Q, etc.) txt, which includes many sections
    txt = r.text
    
    # Extract SEC header into a raw string
    header = txt[:txt.find('</SEC-HEADER>')]

    # Regex to find <DOCUMENT> tags
    doc_start_pattern = re.compile(r'<DOCUMENT>')
    doc_end_pattern = re.compile(r'</DOCUMENT>')
    # Regex to find <TYPE> tag preceding any characters, terminating at new line
    type_pattern = re.compile(r'<TYPE>[^\n]+')

    # There are many <Document> Tags in this text file, each as specific exhibit like 10-K, XML, EX-10.17, etc.
    # First filter will give us document tag start <end> and document tag end's <start>
    # We will use this to later grab content in between these tags
    doc_start_is = [xx.end() for xx in doc_start_pattern.finditer(txt)]
    doc_end_is = [xx.start() for xx in doc_end_pattern.finditer(txt)]

    # Type filter looks for <TYPE> with Not flag as new line (i.e., terminate there)
    # The '+' sign looks for any char afterwards until new line \n
    #   ... which will give us <TYPE> followed with section name like '10-K', '10-Q'
    doc_types = [xx[len('<TYPE>'):] for xx in type_pattern.findall(txt)]

    # Create a loop to go through each section type and save only the main section in the dictionary
    doc_list = []
    for doc_type, doc_start, doc_end in zip(doc_types, doc_start_is, doc_end_is):
        if doc_type == form_type:
            doc = {'doc_type': doc_type, 'doc_start': doc_start, 'doc_end': doc_end, 'doc_txt': txt[doc_start:doc_end]}
            # Avoid filings in pdf/jpeg format
            format_pattern = r"<FILENAME>(.*?)\n<"
            if re.search('(htm|txt)', re.findall(format_pattern, doc['doc_txt'])[0]): # htm/txt, or reject types like pdf/jpeg/...
                doc_list.append(doc)

    # Extract the corresponding HTML scripts of the form (e.g., 10-K, 10-Q)
    if len(doc_list) == 1:
        txt = doc_list[0]['doc_txt']
    elif len(doc_list) > 1:
        print('Multiple records found for form %s in %s, kept the longest one' % (form_type, url))
        doc_list = sorted(doc_list, key=lambda xx: len(xx['doc_txt']), reverse=True)
        txt = doc_list[0]['doc_txt']
    else:
        print('No record found for form %s in %s' % (form_type, url))
        return None

    end_time = time.time()
    print('%s filing %s finished processing in %.2f seconds.' % (form_type, url, end_time - start_time))

    return header, txt

# Parallel processing wrapper of the core function normalize_txt()
def download_txts(urls, form_types, include_ex=False, max_workers=4):

    to_return = dict.fromkeys(urls)
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(download_txt, u, ft, include_ex):\
                             (u, ft) for u, ft in zip(urls, form_types)}
        for future in as_completed(future_to_url):
            url = future_to_url[future][0]
            try:
                header, text = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
            else:
                to_return[url] = {'header': header, 'text': text}
    end_time = time.time()
    print('%d filings finished downloading in %.2f seconds.' % (len(urls), end_time - start_time))
    return to_return


# %% Main
if __name__ == '__main__':
    # Input the Edgar search results saved from the get_urls()
    in_file = sys.argv[1]+'/'+sys.argv[1]+'_'+sys.argv[2]+'_'+sys.argv[2]+'.csv'
    out_file = in_file.replace('.csv', '.json')
    df_urls = pd.read_csv('Catalogs/'+in_file)
    if len(sys.argv) == 5:
        out_file = '_'.join([in_file.replace('.csv', ''), sys.argv[3], sys.argv[4]])+'.json'
        df_urls = df_urls.iloc[int(sys.argv[3]):int(sys.argv[4]), :]
    urls, form_types = df_urls['fname'].values, df_urls['form'].values
    # Request and download Edgar filings
    js_urls = download_txts(urls, form_types) 
    # Write to a json file
    if os.path.exists('Sources'):
        pass
    else:
        os.mkdir('Sources')
    if os.path.exists('Sources/'+sys.argv[1]):
        pass
    else:
        os.mkdir('Sources/'+sys.argv[1])
    with open('Sources/'+out_file, 'w', encoding='utf-8') as f:
        json.dump(js_urls, f)
    f.close()

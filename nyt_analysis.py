import json
import concurrent.futures
import csv
from concurrent.futures import as_completed
from nltk.tokenize import wordpunct_tokenize
from nltk.probability import FreqDist
from nltk.stem.snowball import EnglishStemmer
import re
import threading

def main():
    global etym_data, sns, lock
    etym_data = dict()
    sns = EnglishStemmer()
    sns.stem("load")
    lock = threading.Lock()

    with open('./data/etymology.csv', 'r') as etym_file:
        for etym_line in csv.DictReader(etym_file):
            etym_data[make_tag(etym_line['lang'], etym_line['term'])] = {
                'origin': etym_line['origin'],
                'origin_lang': etym_line['origin_lang']
            }
    global lang_codes
    lang_codes = dict()
    with open('./data/wiktionary_codes.csv', 'r') as codes_file:
        for row in csv.DictReader(codes_file):
            lang_codes[row['code']] = row['lang']

    token_list = []
    token_counts_by_year = dict()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as token_count_pool:
        token_count_futures = []
        for i in range(1852, 1853):
            token_counts_by_year[i] = FreqDist()
            token_count_futures.append(token_count_pool.submit(analyze_year, i))

        for future in as_completed(token_count_futures):
            year, token_count = future.result()
            token_counts_by_year[year].update(token_count)
            token_list = list(set().union(token_list, token_count.keys()))

    token_origins = dict()
    global chunk_size
    chunk_size = 100
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as token_origin_pool:
        token_origin_futures = [
            token_origin_pool.submit(
                find_origins, 
                token_list[i * chunk_size:(i + 1) * chunk_size], i
            ) 
            for i in range(int(len(token_list) / chunk_size) + 1)
        ]

        for future in as_completed(token_origin_futures):
            origins = future.result()
            token_origins.update(origins)


    with open('./data/origin_counts.csv', 'w') as file:
        writer = csv.DictWriter(file, fieldnames=["code", "lang_name", "count", "year"])
        writer.writeheader()
        writer.writerows(make_origin_rows(token_counts_by_year, token_origins))
        
    with open('./data/tag_counts.csv', 'w') as file:
        writer = csv.DictWriter(file, fieldnames=["code", "lang_name", "eng_token", "etym_token", "count", "year"])
        writer.writeheader()
        writer.writerows(make_token_rows(token_counts_by_year, token_origins))

def find_origins(list, i):
    print(f"finding origins of tokens[{i * chunk_size}:{(i + 1) * chunk_size}], len: {len(list)}")
    origins = dict()
    for token in list:
        origin = find_origin(token)
        if origin != '':
            origins[token] = origin

    print(f"finished origins of tokens[{i * chunk_size}:{(i + 1) * chunk_size}]")
    return origins

def flatten(l):
    _l = l
    while type(_l) == list and type(_l[0]) == list:
        _l = [item for sublist in _l for item in sublist]
    return _l

def make_token_rows(token_counts_by_year, token_origins):
    #["code", "lang_name", "eng_token", "etym_token", "count", "year"]
    rows = []
    
    for year in token_counts_by_year:
        for token in token_counts_by_year[year]:
            if token in token_origins:
                orig = token_origins[token]
                code, etym_token = orig.split("_")

                rows.append({
                    'code': code,
                    'lang_name': lang_codes[code],
                    'eng_token': token,
                    'etym_token': etym_token,
                    'count': token_counts_by_year[year][token],
                    'year': year
                }) 

    return rows

def make_origin_rows(token_counts_by_year, token_origins):
    #["code", "lang_name", "count", "year"]
    rows = []
    orig_counts = dict()

    for year in token_counts_by_year:
        if year not in orig_counts:
            orig_counts[year] = dict()

        for token in token_counts_by_year[year]:
            if token in token_origins:
                origin = token_origins[token]
                code = origin.split("_")[0]
                orig_counts[year][code] = orig_counts[year][code] + 1 if code in orig_counts[year] else 1
    
    for year in orig_counts:
        for orig in orig_counts[year]:
            rows.append({
                'count': orig_counts[year][orig],
                'code': orig,
                'lang_name': lang_codes[orig],
                'year': year
            })

    return rows

def analyze_year(i):
    print(f"analyzing year {i}")
    token_counts = FreqDist()

    with open(f"./data/nyt/{i}.json") as file:
        article_data = json.load(file)

        for article in article_data["response"]["docs"]:
            words = collect_words(article)
            token_counts.update(FreqDist(words))

    print(f"completed year {i}")
    return i, token_counts

def not_punct_or_numeric(x):
    return not re.match("[\.|;|\!|\?|\'|\"|\:|\||\,|\-|_|\+|\(|\)|\=|\/|\<|\>|@|#|\$|%|\^|\&|\*|\-\-]+",x)\
          and not x.isnumeric()

def collect_words(article):
    tokenized_list = [wordpunct_tokenize(x) for x in 
            [article["abstract"], article["headline"]["main"], article["lead_paragraph"]]]
    word_list = filter(not_punct_or_numeric, flatten(tokenized_list))
    return [sns.stem(x.lower()) for x in word_list]

def make_tag(code, token):
    return f"{code}_{token}"

def find_origin(token):
    tag = make_tag("en", token)
    next_tag = ''
    tags = [tag]

    if tag in etym_data:
        next_tag = make_tag(etym_data[tag]['origin_lang'], etym_data[tag]['origin'])
        while next_tag in etym_data and etym_data[next_tag]['origin'] != '' and next_tag not in tags:
            tags.append(tag)
            tag = next_tag
            next_tag = make_tag(etym_data[tag]['origin_lang'], etym_data[tag]['origin'])

    return next_tag

if __name__ == "__main__":
    main()

import json
import concurrent.futures
import csv
from concurrent.futures import as_completed
from functools import reduce
from operator import concat
from nltk import WordNetLemmatizer
from nltk.tokenize import wordpunct_tokenize
import re

def main():
    global etym_data, wnl
    etym_data = dict()
    wnl = WordNetLemmatizer()
    wnl.lemmatize("load")
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2020-1851)

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

    tag_counts = dict()
    futures = []
    for i in range(1852, 1853):
        tag_counts[i] = dict()
        futures.append(pool.submit(analyze_year, i))

    for future in as_completed(futures):
        year, tags = future.result()
        tag_counts[year] = sum_dict(tag_counts[year], tags)

    with open('./data/origin_counts.csv', 'w') as file:
        writer = csv.DictWriter(file, fieldnames=["code", "lang_name", "count", "year"])
        writer.writeheader()
        writer.writerows(flatten(make_origin_rows(year, tag_counts[year]) for year in tag_counts))
        
    with open('./data/tag_counts.csv', 'w') as file:
        writer = csv.DictWriter(file, fieldnames=["code", "lang_name", "token", "count", "year"])
        writer.writeheader()
        writer.writerows(flatten(make_tag_rows(year, tag_counts[year]) for year in tag_counts))

def flatten(arr):
    return reduce(concat, arr)

def sum_dict(sums, tags):
    for tag in tags:
        sums[tag] = sums[tag] + tags[tag] if tag in sums else tags[tag]
    return sums

def make_tag_rows(year, tags):
    rows = []
    for tag in tags:
        tag_split = tag.split("_")
        rows.append({
            'code': tag_split[0],
            'lang_name': lang_codes[tag_split[0]],
            'token': tag_split[1],
            'count': tags[tag],
            'year': year
        })
    return rows

def make_origin_rows(year, tags):
    rows = []
    counts = dict()
    for tag in tags:
        code = tag.split("_")[0]
        counts[code] = counts[code] + tags[tag] if code in counts else tags[tag]
        
    for code in counts:
        rows.append({
            'code': code,
            'count': counts[code],
            'lang_name': lang_codes[code],
            'year': year
        })
        
    return rows

def analyze_year(i):
    print(f"analyzing year {i}")
    with open(f"./data/nyt/{i}.json") as file:
        article_data = json.load(file)
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(article_data["response"]["docs"]))

        futures = []
        tag_counts_year = dict()

        for article in article_data["response"]["docs"]:
            future = pool.submit(analyze_article, article)
            futures.append(future)

        for future in as_completed(futures):
            tags = future.result()
            tag_counts_year = sum_dict(tag_counts_year, tags)

    print(f"completed year {i}")
    return i, tag_counts_year

def not_punct_or_numeric(x):
    return not re.match("[\.|;|\!|\?|\'|\"|\:|\||\,|\-|_|\+|\(|\)|\=|\/|\<|\>|@|#|\$|%|\^|\&|\*|\-\-]+",x)\
          and not x.isnumeric()

def collect_words(article):
    tokenized_list = [wordpunct_tokenize(x) for x in 
            [article["abstract"], article["headline"]["main"], article["lead_paragraph"]]]
    word_list = filter(not_punct_or_numeric, flatten(tokenized_list))
    return [x.lower() for x in word_list]

def make_tag(code, token):
    return f"{code}_{token}"

def analyze_article(article):
    print(f"analyzing article {article['headline']['main']}")
    words = collect_words(article)
    # print(article['headline']['main'], words, len(words))

    tag_counts_article = dict()
    
    for word in words:
        tag = make_tag("en", wnl.lemmatize(word))

        #issue of came

        if tag in etym_data:
            next_tag = make_tag(etym_data[tag]['origin_lang'], etym_data[tag]['origin'])
            while next_tag in etym_data and etym_data[next_tag]['origin'] != '':
                tag = next_tag
                next_tag = make_tag(etym_data[tag]['origin_lang'], etym_data[tag]['origin'])
            tag_counts_article[next_tag] = tag_counts_article[next_tag] + 1 if next_tag in tag_counts_article else 1

    print(f"completed article {article['headline']['main']}")
    return tag_counts_article

if __name__ == "__main__":
    main()

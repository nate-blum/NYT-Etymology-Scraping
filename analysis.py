import json
import concurrent.futures
import csv
from concurrent.futures import as_completed
import re
from functools import reduce
from operator import concat
import pandas

# TODO: watch for stuff like regenc[???]d and -- em dashes
# proper nouns

def main():
    global etym_data
    etym_data = dict()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2020-1851)

    with open('./data/origin_data.csv', 'r') as file:
        for line in csv.reader(file):
            etym_data[line[1]] = {
                'lang': line[2],
                'origin': line[3],
                'origin_lang': line[4]
            }

    etym_counts = dict()
    with open('./data/iso_639-2.csv', 'r') as file:
        for i in range(1852, 1853):
            for line in csv.reader(file):
                etym_counts[i] = dict()
                etym_counts[i][line[0]] = {
                    'code': line[0],
                    'count': 0,
                    'name': line[3]
                }

    futures = []

    for i in range(1852, 1853):
        future = pool.submit(analyze_year, i)
        future._year = i
        futures.append(future)

    for future in as_completed(futures):
        etyms = future.result()
        for origin in etyms.keys():
            year = future._year
            etym_counts[year][origin]["count"] += etyms[origin]

    with open('./data/counts.csv', 'w') as file:
        writer = csv.DictWriter(file, fieldnames=["code", "name", "count"])
        writer.writeheader()
        for data in etym_counts.values():
            writer.writerow(data)

def analyze_year(i):
    print(f"analyzing year {i}")
    with open(f"./data/nyt/{i}.json") as file:
        article_data = json.load(file)
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(article_data["response"]["docs"]))

        futures = []
        etym_counts = dict()

        for article in article_data["response"]["docs"]:
            futures.append(pool.submit(analyze_article, article))

        for future in as_completed(futures):
            etyms = future.result()
            for origin in etyms.keys():
                if origin in etym_counts:
                    etym_counts[origin] += etyms[origin]
                else:
                    etym_counts[origin] = etyms[origin]

    print(f"completed year {i}")
    return etym_counts

def collect_words(article):
    word_list = reduce(concat, [re.split(" |-", x) for x in 
            [article["abstract"], article["headline"]["main"], article["lead_paragraph"]]])
    return [str(re.sub("\.|;|:", "", x)).lower() for x in word_list]

def analyze_article(article):
    print(f"analyzing article {article['headline']['main']}")
    words = collect_words(article)

    etym_counts = dict()
    
    for word in words:
        word_form = word

        while word_form in etym_data:
            word_form = etym_data[word_form]["origin"]

        if etym_data[word_form]["origin_lang"] in etym_counts:
            etym_counts[etym_data[word_form]["origin_lang"]] += 1
        else:
            etym_counts[etym_data[word_form]["origin_lang"]] = 1

    print(f"completed article {article['headline']['main']}")
    return etym_counts

if __name__ == "__main__":
    main()

import json
import pandas
import concurrent.futures

# TODO: watch for stuff like regenc[???]d and -- em dashes
# proper nouns

def main():
    global pool, etym_data
    pool = concurrent.futures.ThreadPoolExecutor()
    etym_data = pandas.read_csv("./data/origin_data.csv")

    for i in range(1851, 2020):
        with open(f"./data/nyt/{i}.json") as file:
            article_data = json.load(file)
            pool.submit(analyze_year, article_data)
                

def analyze_year(data):
    for article in data["response"]["docs"]:
        pool.submit(analyze_article, article)

def analyze_article(article):
    words = 

if __name__ == "main":
    main()

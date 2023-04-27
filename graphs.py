import matplotlib.pyplot as plt
import pandas as pd
import csv

language_groups = [
    "English",
    "German",
    "Latin",
    "Norse",
    "Greek",
    "Itali",
    "Spanish",
    "French",
    "Dutch",
    "Celtic",
    "Persian",
    "Hebrew",
    "Iranian",
    "Scots",
    "Gaelic",
    "Irish",
    "Swedish",
    "Saxon",
    "Slavic",
    "Serbo-Croatian",
    "Semitic",
    "Portuguese",
    "Arabic",
    "Chinese",
    "Danish"
]

def origin_frequency_by_num_words(df):
    for year in range(1852, 2019, 50):
        # year = 1852
        total = df.loc[df['year'] == year]['count'].sum()
        #0.1% threshold
        filtered = df.loc[(df['year'] == year) & ((df['count'] / total) > 0.001)].sort_values(by="count", ascending=False)
        ranks = range(1, len(filtered.index) + 1)
        plt.figure(figsize=(6, 6))
        plt.subplots_adjust(bottom=0.3)
        plt.bar(ranks, filtered['count'])
        plt.xlabel(f"Origin rank for {year}")
        plt.ylabel(f"Origin frequency by # words for {year}") 
        plt.xticks(ticks=ranks, labels=filtered['lang_name'], rotation="vertical") 
        # plt.show()
        plt.savefig(f"./data/figs/origin_freq_by_num_words_{year}.png")

def origin_relative_proportion_by_num_words(df):
    for year in range(1852, 2019, 50):
        # year = 1852
        total = df.loc[df['year'] == year]['count'].sum()
        #0.1% threshold
        filtered = df.loc[(df['year'] == year) & ((df['count'] / total) > 0.001)].sort_values(by="count", ascending=False)
        ranks = range(1, len(filtered.index) + 1)
        plt.figure(figsize=(6, 6))
        plt.subplots_adjust(bottom=0.3)
        bar = plt.bar(ranks, list(map(lambda x: (x / total) * 100, filtered['count'])))
        plt.xlabel(f"Origin rank for {year}")
        plt.ylabel(f"Origin relative proportion by # words for {year}") 
        plt.xticks(ticks=ranks, labels=filtered['lang_name'], rotation="vertical") 
        # plt.show()
        plt.savefig(f"./data/figs/origin_rel_prop_by_num_words_{year}.png")

def origin_relative_proportion_over_time(df):
    plt.figure(figsize=(10,6))
    for lang in df['lang_name'].unique():
        x = []
        y = []

        for year in range(1852, 2020):
            total = df.loc[df['year'] == year]['count'].sum()
            row = df.loc[(df['year'] == year) & (df['lang_name'] == lang)]
            if len(row.values) > 0 and row.values[0][1] / total > 0.001:
                x.append(year)
                y.append((row.values[0][1] / total) * 100)

        if len(x) > 0 and len(y) > 0:
            plt.plot(x, y, label=lang)
    plt.show()


with open('./data/aggregated_origin_counts.csv', 'r') as file:
    rows = csv.DictReader(file)
    df = pd.DataFrame(rows, columns=["lang_name","count","year"])
    df = df.astype({ 'year': 'int', 'count': 'int' })
    print(df)

    # origin_frequency_by_num_words(df)
    # origin_relative_proportion_by_num_words(df)
    origin_relative_proportion_over_time(df)


    

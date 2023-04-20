import csv
import pandas

with open("./data/etymwn.tsv") as f:
    rd = csv.reader(f, delimiter="\t", quotechar='"')
    word_origins = pandas.DataFrame(
        data=[[row[0].split(" ")[1], 
               row[0].split(" ")[0].replace(":", ""), 
               row[2].split(" ")[1], 
               row[2].split(" ")[0].replace(":", "")] for row in [x for x in rd if "etymology" in x[1]]],
        columns=["word", "word_lang_code", "origin", "origin_lang_code"]
        )

    word_origins.to_csv(open("./data/origin_data.csv", 'w'))
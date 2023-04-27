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

def add_or_set(dict, key, value):
    if key in dict:
        dict[key] += int(value)
    else:
        dict[key] = int(value)

with open('./data/origin_counts.csv', 'r') as i_file:
    with open('./data/aggregated_origin_counts.csv', 'w') as o_file:
        write = csv.DictWriter(o_file, fieldnames=["lang_name", "count", "year"])
        write.writeheader()

        rows = csv.DictReader(i_file)
        etym_counts = dict()
        for row in rows:
            if row['year'] not in etym_counts:
                etym_counts[row['year']] = dict()

            group = [x for x in language_groups if x in row['lang_name']]
            if len(group) > 0:
                add_or_set(etym_counts[row['year']], group[0], row['count'])
            else:
                add_or_set(etym_counts[row['year']], row['lang_name'], row['count'])

        for year in etym_counts:
            write.writerows([{
                'lang_name': lang,
                'count': etym_counts[year][lang],
                'year': year
            } for lang in etym_counts[year]])

# with open('./data/tag_counts.csv', 'r') as i_file:
#     with open('./data/aggregated_tag_counts.csv', 'w') as o_file:
#         write = csv.DictWriter(o_file, fieldnames=["lang_name", "eng_token", "etym_token", "count", "year"])
#         write.writeheader()

#         rows = csv.DictReader(i_file)
#         etym_counts = dict()
#         for row in rows:
#             if row['year'] not in etym_counts:
#                 etym_counts[row['year']] = dict()

#             group = [x for x in language_groups if x in row['lang_name']]
#             if len(group) > 0:
#                 etym_counts[row['year']].update({ group: row['count'] })
#             else:
#                 etym_counts[row['year']].update({ row['lang_name']: row['count'] })

#         for year in etym_counts:
#             write.writerows([{
#                 'lang_name': lang,
#                 'count': etym_counts[year][lang],
#                 'year': year
#             } for lang in etym_counts[year]])

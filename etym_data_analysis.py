import csv
from functools import reduce
from operator import concat

type_blacklist = [
    'cognate_of',
    'is_onomatopoeic',
    'initialism_of',
    'abbreviation_of',
    'group_affix_root',
    'group_related_root',
    'group_derived_root'
]

#etym_analysis_by_reltype
eabr = dict()
#etym_analysis_by_origin
eabo = dict()

def flatten(l):
    _l = l
    while type(_l) == list and type(_l[0]) == list:
        _l = [item for sublist in _l for item in sublist]
    return _l

with open("./data/etymology_orig.csv", "r") as file:
    for row in csv.DictReader(file):
        lang, rel_type, rel_lang = row["lang"], row["reltype"], row["related_lang"]
        if rel_type in type_blacklist:
            continue

        if lang in eabo:
            if rel_lang in eabo[lang]:
                if rel_type in eabo[lang][rel_lang]:
                    eabo[lang][rel_lang][rel_type]["count"] += 1
                else:
                    eabo[lang][rel_lang][rel_type] = { "count": 1 }
            else:
                eabo[lang][rel_lang] = dict()
        else:
            eabo[lang] = dict()

        if lang in eabr:
            if rel_type in eabr[lang]:
                if rel_lang in eabr[lang][rel_type]:
                    eabr[lang][rel_type][rel_lang]["count"] += 1
                else:
                    eabr[lang][rel_type][rel_lang] = { "count": 1 }
            else:
                eabr[lang][rel_type] = dict()
        else:
            eabr[lang] = dict()
        
    with open("./data/etym_analysis_eabo.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=['count', 'lang', 'rel_lang', 'rel_type'])
        writer.writeheader()
        writer.writerows(flatten([
            [
                [
                    {
                        'count': eabo[lang][rel_lang][rel_type]["count"],
                        'rel_lang': rel_lang,
                        'rel_type': rel_type,
                        'lang': lang
                    } for rel_type in eabo[lang][rel_lang]
                ] for rel_lang in eabo[lang]
            ] for lang in eabo
        ]))

    with open("./data/etym_analysis_eabr.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=['count', 'lang', 'rel_type', 'rel_lang'])
        writer.writeheader()
        writer.writerows(flatten([
            [
                [
                    {
                        'count': eabr[lang][rel_type][rel_lang]["count"],
                        'rel_lang': rel_lang,
                        'rel_type': rel_type,
                        'lang': lang
                    } for rel_lang in eabr[lang][rel_type]
                ] for rel_type in eabr[lang]
            ] for lang in eabr
        ]))
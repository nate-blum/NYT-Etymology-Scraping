import csv
import itertools
import statistics

type_priorities = {
    'inherited_from': 7, 
    'derived_from': 8, 
    'borrowed_from': 6, 
    'group_derived_root': 5,
    'learned_borrowing_from': 5, 
    'semi_learned_borrowing_from': 5, 
    'orthographic_borrowing_from': 5, 
    'unadapted_borrowing_from': 5,
    'calque_of': 5,
    'semantic_loan_of': 5,
    'has_prefix_with_root': 4,
    'has_suffix_with_root': 4,
    'back-formation_from': 3,
    'compound_of': 2,
    'blend_of': 2,
    'clipping_of': 1,
    'abbreviation_of': 1,
    'group_derived_root': 0
}

def single_priority(etym):
    if etym['type'] in type_priorities:
        return type_priorities[etym['type']]
    else:
        return 0

def compute_group_priority(group):
    return statistics.mean(list(map(
        lambda x: single_priority(x),
        group
    ))) if len(group) > 0 else 0

def get_priority(etym):
    if type(etym) == list:
        return compute_group_priority(etym)
    else:
        return single_priority(etym)

def max_priority(etyms):
    if not etyms:
        return None

    max = etyms[list(etyms.keys())[0]]

    for tag in etyms:
        if get_priority(etyms[tag]) > get_priority(max):
            max = etyms[tag]

    return (max[len(max) - 1] if len(max) > 0 else None) if type(max) == list else max

def make_etym(row):
    return  {
                'lang': wk_codes[row['lang']],
                'lang_name': row['lang'],
                'term': row['term'],
                'origin_lang': wk_codes[row['related_lang_full']],
                'origin': row['related_term'],
                'type': row['reltype']
            }

def make_rows(etym_data):
    return filter(lambda x: x, [max_priority(etym_data[tag]) for tag in etym_data])

with open('./data/wiktionary_codes.csv', 'r') as codes:
    wk_codes = dict()
    for line in csv.DictReader(codes):
        wk_codes[line['lang']] = line['code']

    with open("./data/etymology_orig.csv", "r") as file:
        tag_etyms = dict()
        for row in csv.DictReader(file):
            if row['reltype'] in type_priorities and\
                row['lang'] in wk_codes and not wk_codes[row['lang']].startswith('#'):
                tag = f"{wk_codes[row['lang']]}-{row['term']}"

                if tag not in tag_etyms:
                    tag_etyms[tag] = dict()

                if row['reltype'] == 'group_derived_root':
                    tag_etyms[tag][row['group_tag']] = []
                elif row['related_lang_full'] in wk_codes and not wk_codes[row['related_lang_full']].startswith('#') and\
                    row['definition_num'] == '0':
                    if row['parent_tag'] != '' and row['parent_tag'] in tag_etyms[tag]:
                        tag_etyms[tag][row['parent_tag']].append(make_etym(row))
                    else:
                        tag_etyms[tag][row['related_term_id']] = make_etym(row)
            
        with open("./data/etymology.csv", "w") as f:
            writer = csv.DictWriter(f, fieldnames=["lang_name", "lang", "term", "origin_lang", "origin", "type"])
            writer.writeheader()
            writer.writerows(make_rows(tag_etyms))
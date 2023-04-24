import csv

type_priority = {
    'inherited_from': 2, 
    'derived_from': 1, 
    'borrowed_from': 3, 
    'learned_borrowing_from': 4, 
    'semi_learned_borrowing_from': 4, 
    'orthographic_borrowing_from': 4, 
    'unadapted_borrowing_from': 4,
    'calque_of': 4,
    'semantic_loan_of': 4,
    'has_prefix_with_root': 5,
    'has_suffix_with_root': 5,
    'back-formation_from': 6,
    'compound_of': 7,
    'blend_of': 7,
    'clipping_of': 8,
    'abbreviation_of': 8
}

lang_priority = {
    'English': 1,
    'French': 2,
    'German': 3,
    'Latin': 4,
    'Greek': 5
}

def get_lang_priority(lang):
    for lang_type in lang_priority:
        if lang_type in lang:
            return lang_priority[lang_type]
    return 6

def compute_priority(etym):
    return get_lang_priority(etym['lang_name']) + type_priority[etym['type']]

def min_priority(etyms):
    min_etym = etyms[0]
    for etym in etyms:
        if etym['term'] == "or":
            print(etym, min_etym, compute_priority(etym), compute_priority(min_etym))
        if compute_priority(etym) < compute_priority(min_etym):
            min_etym = etym
    return min_etym

with open('./data/wiktionary_codes.csv', 'r') as codes:
    wk_codes = dict()
    for line in csv.DictReader(codes):
        wk_codes[line['lang']] = line['code']

    with open("./data/etymology_orig.csv", "r") as file:
        etym_data = []

        #priorities for etymologies:
        #TODO age of language priority 
        #separate entry, major language, good relation
        #group entry, major language, good relation (include all group entries, organized as separate entries)
        #separate entry, non-major language, good relation
        #group entry, non-major language, good relation
        #same but for bad relations
        #priorities:
        #0 for group entry, 100 for separate entry
        #0 for non-major language, 10 for major language
        #0 for bad relation, 1 for good relation

        tag_etyms = dict()
        for row in csv.DictReader(file):
            if row['reltype'] in type_priority and\
                (row['related_lang'] in wk_codes and row['lang'] in wk_codes) and\
                (not wk_codes[row['related_lang']].startswith('#') and not wk_codes[row['lang']].startswith('#')) and\
                row['parent_position'] == '':
                
                tag = f"{row['lang']}-{row['term']}"
                if tag not in tag_etyms:
                    tag_etyms[tag] = []

                tag_etyms[tag].append({
                    'lang': wk_codes[row['lang']],
                    'lang_name': row['lang'],
                    'term': row['term'].lower(),
                    'origin_lang': wk_codes[row['related_lang']],
                    'origin': row['related_term'].lower(),
                    'type': row['reltype']
                })

        for etym in tag_etyms:
            etym_data.append(min_priority(tag_etyms[etym]))
            
        with open("./data/etymology.csv", "w") as f:
            writer = csv.DictWriter(f, fieldnames=["lang_name", "lang", "term", "origin_lang", "origin", "type"])
            writer.writeheader()
            writer.writerows(etym_data)
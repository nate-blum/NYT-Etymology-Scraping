import csv

types = {
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

def min_priority_type(etyms):
    min_etym = etyms[0]
    for etym in etyms:
        if types[etym['type']] < types[min_etym['type']]:
            min_etym = etym
    return min_etym

with open('./data/wiktionary_codes.csv', 'r') as codes:
    wk_codes = dict()
    for line in csv.DictReader(codes):
        wk_codes[line['lang']] = line['code']

    with open("./data/etymology_orig.csv", "r") as file:
        etym_data = []

        local_etyms = []
        tag = ""
        for row in csv.DictReader(file):
            if row['reltype'] in types and\
                (row['related_lang'] in wk_codes and row['lang'] in wk_codes) and\
                (not wk_codes[row['related_lang']].startswith('#') and not wk_codes[row['lang']].startswith('#')):
                curr_tag = f"{row['lang']}-{row['term']}"

                if curr_tag != tag:
                    if len(local_etyms) > 0:
                        etym_data.append(min_priority_type(local_etyms))
                    local_etyms = []
                    tag = curr_tag

                local_etyms.append({
                    'lang': wk_codes[row['lang']],
                    'term': row['term'].lower(),
                    'origin_lang': wk_codes[row['related_lang']],
                    'origin': row['related_term'].lower(),
                    'type': row['reltype']
                })
            
        with open("./data/etymology.csv", "w") as f:
            writer = csv.DictWriter(f, fieldnames=["lang", "term", "origin_lang", "origin", "type"])
            writer.writeheader()
            writer.writerows(etym_data)
import sqlite3
import json
from dict.scripts.paths import DB_PATH, THIS_DIR
from collections import defaultdict

def spanify(word, start, end):
    return word[:start] + "{" + word[start:end] + "}" + word[end:]

def spanify_all(word, data):
    span_list = sorted([x["span"] for x in data], key=lambda x: x[0], reverse=True)
    for s, e in span_list:
        word = spanify(word, s, e)
    return word

def extract_morpheme(spanified, syllabified, start, lemmas, debug=False):
    if spanified == "eftersä{g}are":
        return "sä{g}a"
    if "na{ti}on" in spanified:
        return "na{ti}on"
    if "|" not in syllabified:
        return spanified
    
    target_morpheme = 0
    syllabified = syllabified.replace("↗", "").replace("↘", "")
    i = 0
    for char in syllabified:
        if char == "|":
            target_morpheme += 1
            continue
        if i == start:
            break
        i += 1
    morphemes = syllabified.split("|")
    target_susbstr = morphemes[target_morpheme]
    result_substr = None
    for i in range(len(spanified)):
        if result_substr:
            break
        for j in range(1, len(target_susbstr) + 3):
            candidate = spanified[i:i+j]
            if not ("{" in candidate and "}" in candidate):
                continue
            if candidate.replace("{", "").replace("}", "") == target_susbstr:
                result_substr = candidate
                break

    if not result_substr:
        raise ValueError(f"Couldn't find {target_susbstr} in {spanified}")
    
    if result_substr == "{ch}ans":
        pass
    elif result_substr.endswith("s") and result_substr[:-1] in lemmas:
        return result_substr[:-1]
    return result_substr.strip()
        

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    irreg_types = {}

    c.execute("""
        SELECT w.lemma, b.word, b.syllables, b.irregular_pron
        FROM sv_wiktionary w
        JOIN braxen b ON CAST(w.braxen_ids AS INTEGER) = b.id
        WHERE w.braxen_ids NOT LIKE '%,%'
            AND b.irregular_pron IS NOT NULL
            AND (w.lemma = b.word OR w.slot IN ('IND_PL', 'DEF_SG'))
        ORDER BY w.id -- prioritizes lemmas
    """)
    rows = {lemma.lower(): (lemma.lower(), word, syllables, irreg)
            for lemma, word, syllables, irreg in c.fetchall()}
    
    for lemma, word, syllables, irreg_pron in rows.values():
        irreg_pron_data = json.loads(irreg_pron)

        for x in irreg_pron_data:
            s, e = x["span"]
            span_type = x["type"]
            spanified_word = spanify(word, s, e)
            spanified_lemma = spanify(lemma, s, e)
            
            if span_type not in irreg_types:
                irreg_types[span_type] = defaultdict(set)

            winner = extract_morpheme(spanified_word, syllables, s, rows.keys())
            if span_type.startswith("hard k") and any(winner.endswith(ending) 
                                                      for ending in {"{k}en", "{k}el", "{k}e", "{k}e-"}):
                continue
            if word.lower() != lemma:
                winner_lemma = None
                i = 1
                while not winner_lemma and i <= 3:
                    try:
                        winner_lemma = extract_morpheme(spanified_lemma, syllables[:-i], s, rows.keys())
                    except ValueError:
                        i += 1
                        continue
                irreg_types[span_type][winner_lemma].add(spanified_lemma)
            else:
                irreg_types[span_type][winner].add(spanified_word)

    with open(THIS_DIR.parent / "irreg_pron_src.md", "w") as f:
        f.write("# Irregular pronunciations\n\n")

        i = 0
        for span_type, data in irreg_types.items():
            if i:
                f.write("\n")
            f.write(f"## {span_type} ({len(data)})\n\n")
            for morpheme, words in sorted(data.items(), 
                                          key=lambda x: x[0].lower().replace("{", "").replace("}", "")):
                if len(words) > 1 or morpheme not in words:
                    f.write(f"- **{morpheme}** ({len(words)}): {', '.join(sorted(words))}\n")
                else:
                    f.write(f"- **{morpheme}**\n")
            i += 1


if __name__ == "__main__":
    main()

# Impordid
from estnltk.corpus_processing.parse_enc import parse_enc_file_iterator
import os
from operator import itemgetter
from datetime import datetime

# Kaustas "korpus" on 2017 koondkorpuse failid estonian_nc17.vert.01 - estonian_nc17.vert.25
# Õpetus ja koondkorpuste failide lingid: https://github.com/estnltk/estnltk/blob/main/tutorials/corpus_processing/importing_text_objects_from_corpora.ipynb
input_folder = "korpus"

# Käime läbi kõik korpuse failid ning loendame kokku lemmad Pythoni sõnastiku abil
lemmad = dict()
for filename in os.listdir(input_folder):
    file_path = os.path.join(input_folder, filename)
    print(filename) # Jälgimiseks
    l = 0
    for text_obj in parse_enc_file_iterator(file_path, restore_morph_analysis = True):
        if "lang" in text_obj.meta:
            if text_obj.meta["lang"] != "Estonian": # Muus keeles tekstid jätame vahele
                continue
        for lemma in [lem[0] for lem in text_obj["original_morph_analysis"]["lemma"]]:
            if lemma in lemmad.keys():
                lemmad[lemma] = lemmad[lemma] + 1 # Kui lemma on sõnastikus suurendame loendust ühe võrra
            else:
                lemmad[lemma] = 1 # Kui lemmat veel pole sõnastikus, lisatakse see juurde

        if l % 10000 == 0: # Jälgimiseks palju tekstidest on läbi käidud (ühes koondkorpuse failis kuni 250 000 teksti)
            print(l)
            t = datetime.now()
            print(t)
        l += 1

N = 50000 # Mitu kõige sagedasemat lemmat leida
res = dict(sorted(lemmad.items(), key = itemgetter(1), reverse = True)[:N]) # Sõnastik kõige sagedasemate lemmadega

# Kirjutame dict.txt faili välja kogu sõnastiku - lemmad koos loendustega
with open("dict.txt", "w", encoding = "utf8") as dict_file:
    dict_file.write(str(res))

# Kirjutame vocab_final.txt faili välja N kõige sagedasemat lemmat
with open("vocab_final.txt", "w", encoding = "utf8") as txt_file:
    for lemma in res.keys():
        txt_file.write(lemma + "\n")

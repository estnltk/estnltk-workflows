# Impordid
from estnltk.corpus_processing.parse_enc import parse_enc_file_iterator
import os
import csv
from datetime import datetime

# Kaustas "korpus" on 2017 koondkorpuse failid estonian_nc17.vert.01 - estonian_nc17.vert.25
# Õpetus ja koondkorpuste failide lingid: https://github.com/estnltk/estnltk/blob/main/tutorials/corpus_processing/importing_text_objects_from_corpora.ipynb
input_folder = "korpus"

# Käime läbi kõik korpuse failid ning kirjutame kõik dokumendid koos lausetega välja
# Dokumendi laused on eraldatud sümboliga "\n" ning dokumendid omavahel sümboliga "\t"
# Iga korpuse faili kohta tekib .tsv fail, mida on edaspidi lihtsam töötleda "treeningandmete_loomine.py" scriptis
for filename in os.listdir(input_folder):
    file_path = os.path.join(input_folder, filename)
    read = []
    for text_obj in parse_enc_file_iterator(file_path):
        if "lang" in text_obj.meta:
            if text_obj.meta["lang"] != "Estonian": # Muus keeles tekstid jätame vahele
                continue
        laused = []
        for span in text_obj.original_sentences:
            laused.append(text_obj.text[span.start:span.end])
        read.append("\n".join(laused))

    # Kirjutame estonian_nc17.vert.XX.tsv faili korpuse kausta (XX tähistab korpuse faili indeksit)
    with open(file_path + ".tsv", 'w', newline='', encoding="utf-8") as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        for record in read:
            writer.writerow([record])

    # Jälgimiseks
    print(file_path)
    print(datetime.now())


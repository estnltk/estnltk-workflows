# Impordid
from datasets import load_dataset
from src.transformers import BertTokenizer
from datetime import datetime

# Jälgimiseks
print("1. Scripti käivitamine:")
print(datetime.now())
algus = datetime.now()

# Sisendfailide nimed
# NB! Mõistlik teha batchidena, muidu jookseb script liiga kaua, iga faili töötlemine võtab 12-36 tundi.
input_folder = "korpus"
input_files = ['korpus/estonian_nc17.vert.' + str(i) + '.tsv' for i in range(25)]

# Tokeniseerija
tokenizer = BertTokenizer(vocab_file = "vocab_final.txt", vocab_file_form = "vocab_form.txt", max_length = 128, padding = "max_length", truncation = True, return_tensors = "pt")

# Igas batchis on vaikimisi 16 teksti, igas tekstis laused on eraldatud \n sümboliga --> 'text' : lause1 \n lause2 \n lause3)
# tokeniseeri_batch tokenieerib kõik batchi laused ja lisab batchile labelid
# Väjund (x tähistab kõikide lausete arvu loetud batchi tekstides)
# {'input_ids': x*128*2 tensor, 'attention_mask': x*128 tensor, 'token_type_ids': x*128 tensor, 'binary_channels': x*128*8 tensor, 'labels': x*128*2 tensor}
def tokeniseeri_batch(batch):
    batch_laused = [lause for para in batch["text"] for lause in para.split("\n")]
    tokeniseeritud = tokenizer(batch_laused, max_length = 128, padding = "max_length", truncation = True, return_tensors = "pt")
    tokeniseeritud["labels"] = tokeniseeritud["input_ids"]
    return tokeniseeritud

# Iga korpuse .tsv fail tokeniseeritakse ning kirjutatakse .json kujul faili
# Kausta "korpus2" tekivad failid estonian_nc17.vert.XX.json, kus XX tähistab korpuse faili indeksit
for filename in input_files:
    print(datetime.now())
    print(filename)
    train_dataset = load_dataset("csv", data_files={'train': filename}, names = ["text"], delimiter = "\t")["train"]
    train_dataset_sisend = train_dataset.map(tokeniseeri_batch, batched=True, remove_columns=["text"])
    out_name = "korpus2/" + filename[7:-4] + ".json"
    train_dataset_sisend.to_json(out_name)

lopp = datetime.now()
print("Kestus")
print(lopp-algus)
# Impordid
import os
from datasets import load_dataset
from src.transformers import BertTokenizer, BertForMaskedLM, BertConfig, Trainer, TrainingArguments, DataCollatorForLanguageModeling
from datetime import datetime
import torch

# Jälgimiseks
print("1. Scripti käivitamine:")
print(datetime.now())
algus = datetime.now()

# Võimalusel kasutame treenimiseks GPU-d
print("Cuda: ")
print(torch.cuda.is_available()) # Näitab, kas üldse on GPU seade saadaval
device = "cuda:0" if torch.cuda.is_available() else "cpu"

# Sisendfailide nimed
input_folder = "korpus2"
input_files = [os.path.join(input_folder, filename) for filename in os.listdir(input_folder) if filename[-4:] == "json"]

# Tokeniseerija loomine
tokenizer = BertTokenizer(vocab_file = "vocab_final.txt", vocab_file_form = "vocab_form.txt", max_length = 128,
                          padding = "max_length", truncation = True, return_tensors = "pt", mask_token="ˇMASKˇ")

# Dataseti sisselugemine
treening_andmed = load_dataset("json", data_files={'train': input_files})["train"]
treening_andmed.set_format(type ='torch')

# Jälgimiseks
print("2. Andmestik sisse loetud ja valmis eeltreenimiseks")
print(datetime.now())

# Data collator teeb transformatsiooni andmetega enne mudelisse andmist, antud juhul maskeerib 15% sõnadest
data_collator = DataCollatorForLanguageModeling(
        tokenizer=tokenizer,
        mlm=True,
        mlm_probability=0.15
)

# Konfiguratsioon
config = BertConfig(
    vocab_size = tokenizer.vocab_size,
    vocab_size_form = tokenizer.vocab_size_form,
    tie_word_embeddings = False
)

# Mudeli loomine
model = BertForMaskedLM(config)
model = model.to(device)

# Treeningparameetrite sättimine (EstBERT põhjal)
training_args = TrainingArguments(
    output_dir='./train_results',
    per_device_train_batch_size=32,
    max_steps=900000,
    learning_rate=1e-4,
    logging_dir='./train_logs',
    logging_steps=50000,
    save_steps=50000,
    warmup_steps=9000
)

# Treenija loomine
trainer = Trainer(
    model=model,
    args=training_args,
    data_collator = data_collator,
    train_dataset=treening_andmed
)

# Failitüübi error teatud juhtudel, toores parandus: https://github.com/nlp-with-transformers/notebooks/issues/31
old_collator = trainer.data_collator
trainer.data_collator = lambda data: dict(old_collator(data))

trainer.train()

# Jälgimiseks
print("3. Treenimine lõpetatud")
print(datetime.now())
lopp = datetime.now()
print("Kestus")
print(lopp-algus)
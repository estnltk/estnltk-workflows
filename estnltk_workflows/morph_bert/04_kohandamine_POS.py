# Impordid
import conllu
import numpy as np
import torch
from src.transformers import BertTokenizer, BertForTokenClassification, BertConfig, Trainer, TrainingArguments, DataCollatorForTokenClassification
from transformers import AutoTokenizer, AutoModelForTokenClassification
from sklearn.metrics import classification_report
from datasets import Dataset
from datetime import datetime

# Jälgimiseks
print("1. Scripti käivitamine:")
print(datetime.now())
algus = datetime.now()

# Võimalusel kasutame GPU-d
device = 'cuda' if torch.cuda.is_available() else 'cpu'

# Andmete lugemine
with open("conllu/et_edt-ud-train.conllu", mode="r", encoding="utf8") as f:
    data_raw_train = f.read()

with open("conllu/et_edt-ud-test.conllu", mode="r", encoding="utf8") as f:
    data_raw_test = f.read()

with open("conllu/et_edt-ud-dev.conllu", mode="r", encoding="utf8") as f:
    data_raw_val = f.read()

# Andmete parsimine
data_train = conllu.parse(data_raw_train)
data_test = conllu.parse(data_raw_test)
data_val = conllu.parse(data_raw_val)

# Andmete töötlemine
train_paarid = []
for lause in data_train:
    train_paarid.append({"lause": [(token["form"], token["upos"]) for token in lause]})

test_paarid = []
for lause in data_test:
    test_paarid.append({"lause": [(token["form"], token["upos"]) for token in lause]})

val_paarid = []
for lause in data_val:
    val_paarid.append({"lause": [(token["form"], token["upos"]) for token in lause]})

# Konverteerijate loomine label -- indeks ja indeks -- label
tags = list(set(token[1] for lause in train_paarid for token in lause["lause"]))
tag2idx = {tag:idx for idx, tag in enumerate(tags)}
idx2tag = {idx:tag for idx, tag in enumerate(tags)}

# Tokeniseerija loomine
tokenizer = BertTokenizer(vocab_file = "vocab_final.txt", vocab_file_form = "vocab_form.txt", max_length = 128,
                         padding = "max_length", truncation = True, return_tensors = "pt", mask_token="ˇMASKˇ")

# Kõikide lausete tokeniseerimine ja labelite lisamine
# Kuna tavalisel BERT mudelil pole Fast tokeniseerijat, siis tuleb käsitsi labelid viia vastavusse tokeniseeritud sõnadega
# Ehk tokeniseerime kõik sõnad eraldi, et teada saada, millised tokenid sõnast tekivad
# Seejärel viime need kokku tokeniseeritud lausega, et igal tokenil oleks õige label algandmetest küljes
# Õpetuse põhjal tehtud: https://github.com/Kyubyong/nlp_made_easy/blob/master/Pos-tagging%20with%20Bert%20Fine-tuning.ipynb
def tokeniseeri_lause_lisa_labelid(batch):
    INP, TTI, BIN, ATT, LAB = [], [], [], [], []
    for i, lause_paarid in enumerate(batch["lause"]):
        lause = [x[0] for x in lause_paarid]
        labelid_alg = [x[1] for x in lause_paarid]
        lause_sonade_tokenid = []
        for sona in lause:
            tokeniseeritud_sona = tokenizer(sona, estnltk_first_token=True)
            lause_sonade_tokenid.append(tokeniseeritud_sona["input_ids"][1:-1])

        tokeneid_sonadel = [len(x) for x in lause_sonade_tokenid]
        tokeniseeritud_lause = tokenizer(lause, is_split_into_words=True, max_length=128,
                                         padding="max_length", truncation=True, return_tensors="pt",
                                         estnltk_first_token=True)
        labelid = []
        i = 0
        mitu_id = False
        j = 0

        for input_id in tokeniseeritud_lause["input_ids"][0]:

            if mitu_id:
                labelid.append(-100)
                j -= 1
                if j == 0:
                    mitu_id = False
                continue

            if input_id[0].item() < 5:
                labelid.append(-100)
                continue

            labelid.append(tag2idx[labelid_alg[i]])

            if tokeneid_sonadel[i] > 1:
                j = tokeneid_sonadel[i] - 1
                mitu_id = True

            i += 1

        assert len(tokeniseeritud_lause["input_ids"][0]) == len(labelid)

        INP.append(tokeniseeritud_lause["input_ids"])
        TTI.append(tokeniseeritud_lause["token_type_ids"])
        BIN.append(tokeniseeritud_lause["binary_channels"])
        ATT.append(tokeniseeritud_lause["attention_mask"])
        LAB.append(torch.tensor(labelid))

    INP = torch.cat(INP)
    TTI = torch.cat(TTI)
    BIN = torch.cat(BIN)
    ATT = torch.cat(ATT)
    LAB = torch.stack(LAB)

    encodings = {
        "input_ids": INP,
        "token_type_ids": TTI,
        "binary_channels": BIN,
        "attention_mask": ATT,
        "labels": LAB
    }

    return encodings

# Tokeniseeritud andmestike loomine
# Treeningandmestik
train_dataset = Dataset.from_list(train_paarid)
train_tokenized_dataset = train_dataset.map(tokeniseeri_lause_lisa_labelid, batched=True)

# Valideerimisandmestik
test_dataset = Dataset.from_list(test_paarid)
test_tokenized_dataset = test_dataset.map(tokeniseeri_lause_lisa_labelid, batched=True)

# Testandmestik
val_dataset = Dataset.from_list(val_paarid)
val_tokenized_dataset = val_dataset.map(tokeniseeri_lause_lisa_labelid, batched=True)

# Jälgimiseks
print("2. Andmestik treenimiseks ette valmistatud")
print(datetime.now())

# Mudelite asukohad, mida kohandatakse
mudelid = ["pretrained_models/checkpoint-100000", "pretrained_models/checkpoint-300000", "pretrained_models/checkpoint-500000"]

for i in range(len(mudelid)):

    # Mudeli loomine checkpointist
    model = BertForTokenClassification.from_pretrained(mudelid[i], num_labels=len(tag2idx))
    model.to(device)

    # Data collatori loomine
    data_collator = DataCollatorForTokenClassification(tokenizer)

    # Treeningparameetrite sättimine
    batch_size = 8
    args = TrainingArguments(
        "POS_tag_results" + str((i*2+1)*100000),
        evaluation_strategy="epoch",
        logging_strategy="epoch",
        save_strategy="epoch",
        learning_rate=5e-5,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size,
        num_train_epochs=3
    )

    # Ennustuste tegemise ja täpsuste arvutamise loogika
    def compute_metrics(p):
        predictions, labels = p
        predictions = np.argmax(predictions, axis=2)

        true_predictions = [[tags[p] for (p, l) in zip(prediction, label) if l != -100] for prediction, label in
                            zip(predictions, labels)]
        true_labels = [[tags[l] for (p, l) in zip(prediction, label) if l != -100] for prediction, label in
                       zip(predictions, labels)]

        results = classification_report(np.hstack(true_predictions).tolist(), np.hstack(true_labels).tolist(), output_dict=True)
        return {"precision": results['weighted avg']['precision'], "recall": results['weighted avg']['recall'], "f1": results['weighted avg']['f1-score'], "accuracy": results["accuracy"]}

    # Treenija loomine
    trainer = Trainer(
        model,
        args,
        train_dataset=train_tokenized_dataset,
        eval_dataset=val_tokenized_dataset,
        data_collator=data_collator,
        tokenizer=tokenizer,
        compute_metrics=compute_metrics
    )

    # Treenimine
    trainer.train()

    # Testandmetel ennustamine ja täpsuse arvutamine
    predictions, labels, _ = trainer.predict(test_tokenized_dataset)
    predictions = np.argmax(predictions, axis=2)

    # Arvutame täpsust ainult maskeeritud sõnedel (kus label pole -100)
    true_predictions = [
        [tags[p] for (p, l) in zip(prediction, label) if l != -100]
        for prediction, label in zip(predictions, labels)
    ]
    true_labels = [
        [tags[l] for (p, l) in zip(prediction, label) if l != -100]
        for prediction, label in zip(predictions, labels)
    ]

    results = classification_report(np.hstack(true_predictions).tolist(), np.hstack(true_labels).tolist(), output_dict=True)
    print(results)

# Jälgimiseks
print("3. Mudelid kohandatud")
print(datetime.now())

### Analoogne kood EstBERT mudeli kohandamiseks

# tokenizer = AutoTokenizer.from_pretrained("tartuNLP/EstBERT", max_length=128,
#                                           padding="max_length", truncation=True, return_tensors="pt")
#
#
# def tokeniseeri_lause_lisa_labelid(batch):
#     INP, TTI, ATT, LAB = [], [], [], []
#     for i, lause_paarid in enumerate(batch["lause"]):
#         lause = [x[0] for x in lause_paarid]
#         labelid_alg = [x[1] for x in lause_paarid]
#         lause_sonade_tokenid = []
#         for sona in lause:
#             tokeniseeritud_sona = tokenizer(sona)
#             lause_sonade_tokenid.append(tokeniseeritud_sona["input_ids"][1:-1])
#
#         tokeneid_sonadel = [len(x) for x in lause_sonade_tokenid]
#         tokeniseeritud_lause = tokenizer(lause, is_split_into_words=True, max_length=128,
#                                          padding="max_length", truncation=True, return_tensors="pt")
#         labelid = []
#         i = 0
#         mitu_id = False
#         j = 0
#
#         for input_id in tokeniseeritud_lause["input_ids"][0]:
#
#             if mitu_id:
#                 labelid.append(-100)
#                 j -= 1
#                 if j == 0:
#                     mitu_id = False
#                 continue
#
#             if input_id.item() < 5:
#                 labelid.append(-100)
#                 continue
#
#             labelid.append(tag2idx[labelid_alg[i]])
#
#             if tokeneid_sonadel[i] > 1:
#                 j = tokeneid_sonadel[i] - 1
#                 mitu_id = True
#
#             i += 1
#
#         assert len(tokeniseeritud_lause["input_ids"][0]) == len(labelid)
#
#         INP.append(tokeniseeritud_lause["input_ids"])
#         TTI.append(tokeniseeritud_lause["token_type_ids"])
#         ATT.append(tokeniseeritud_lause["attention_mask"])
#         LAB.append(torch.tensor(labelid))
#
#     INP = torch.cat(INP)
#     TTI = torch.cat(TTI)
#     ATT = torch.cat(ATT)
#     LAB = torch.stack(LAB)
#
#     encodings = {
#         "input_ids": INP,
#         "token_type_ids": TTI,
#         "attention_mask": ATT,
#         "labels": LAB
#     }
#
#     return encodings
#
#
# train_dataset = Dataset.from_list(train_paarid)
# train_tokenized_dataset = train_dataset.map(tokeniseeri_lause_lisa_labelid, batched=True)
#
# test_dataset = Dataset.from_list(test_paarid)
# test_tokenized_dataset = test_dataset.map(tokeniseeri_lause_lisa_labelid, batched=True)
#
# val_dataset = Dataset.from_list(val_paarid)
# val_tokenized_dataset = val_dataset.map(tokeniseeri_lause_lisa_labelid, batched=True)
#
# model = AutoModelForTokenClassification.from_pretrained("tartuNLP/EstBERT", num_labels=len(tag2idx))
# model.to(device)
#
# batch_size = 8
#
# args = TrainingArguments(
#     "POS_tag_results_EstBERT",
#     evaluation_strategy="epoch",
#     logging_strategy="epoch",
#     save_strategy="epoch",
#     learning_rate=5e-5,
#     per_device_train_batch_size=batch_size,
#     per_device_eval_batch_size=batch_size,
#     num_train_epochs=3
# )
#
# data_collator = DataCollatorForTokenClassification(tokenizer)
#
#
# def compute_metrics(p):
#     predictions, labels = p
#     predictions = np.argmax(predictions, axis=2)
#
#     true_predictions = [[tags[p] for (p, l) in zip(prediction, label) if l != -100] for prediction, label in
#                         zip(predictions, labels)]
#     true_labels = [[tags[l] for (p, l) in zip(prediction, label) if l != -100] for prediction, label in
#                    zip(predictions, labels)]
#
#     results = classification_report(np.hstack(true_predictions).tolist(), np.hstack(true_labels).tolist(),
#                                     output_dict=True)
#     return {"precision": results['weighted avg']['precision'], "recall": results['weighted avg']['recall'],
#             "f1": results['weighted avg']['f1-score'], "accuracy": results["accuracy"]}
#
#
# trainer = Trainer(
#     model,
#     args,
#     train_dataset=train_tokenized_dataset,
#     eval_dataset=val_tokenized_dataset,
#     data_collator=data_collator,
#     tokenizer=tokenizer,
#     compute_metrics=compute_metrics
# )
#
# trainer.train()
#
# predictions, labels, _ = trainer.predict(test_tokenized_dataset)
# predictions = np.argmax(predictions, axis=2)
#
# true_predictions = [
#     [tags[p] for (p, l) in zip(prediction, label) if l != -100]
#     for prediction, label in zip(predictions, labels)
# ]
# true_labels = [
#     [tags[l] for (p, l) in zip(prediction, label) if l != -100]
#     for prediction, label in zip(predictions, labels)
# ]
#
# results = classification_report(np.hstack(true_predictions).tolist(), np.hstack(true_labels).tolist(), output_dict=True)
# print(results)

# Jälgimiseks
print("4. Script lõpetatud")
print(datetime.now())
lopp = datetime.now()
print("Kestus")
print(lopp-algus)
# Impordid
import torch
import pandas as pd
import numpy as np
from src.transformers import BertTokenizer, BertForSequenceClassification,Trainer, TrainingArguments
from sklearn.metrics import classification_report
from datasets import Dataset
from sklearn.model_selection import train_test_split
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from datetime import datetime

# Jälgimiseks
print("1. Scripti käivitamine:")
print(datetime.now())
algus = datetime.now()

# Võimalusel kasutame GPU-d
device = 'cuda' if torch.cuda.is_available() else 'cpu'

# Andmete lugemine
andmed = pd.read_csv("Rubric_data/estonianvalence.csv", encoding = "utf8", on_bad_lines='skip', header = None,
                     names = ["rubric","url", "order", "sentiment", "text"])

print(andmed.rubric.value_counts())

# Konverteerijate loomine rubriik -- indeks ja indeks -- rubriik, lisamine andmetele
rubriigid = list(set(andmed["rubric"]))
rubriik_idx = {tag:idx for idx, tag in enumerate(rubriigid)}
idx_rubriik = {idx:tag for idx, tag in enumerate(rubriigid)}
andmed["rubric_idx"] = andmed["rubric"].map(rubriik_idx)

# 70% train, 10% val, 20% test
train, test = train_test_split(andmed, test_size=0.2, stratify=andmed[['rubric_idx']])
train, val = train_test_split(train, test_size=0.125, stratify=train[['rubric_idx']])

# Tokeniseerija loomine
tokenizer = BertTokenizer(vocab_file = "vocab_final.txt", vocab_file_form = "vocab_form.txt", max_length = 128,
                         padding = "max_length", truncation = True, return_tensors = "pt", mask_token="ˇMASKˇ")

# Tokeniseeritud andmestike loomine
# Treeningandmestik
train_encodings = tokenizer(list(train.text), max_length = 128, padding = "max_length",
                            truncation = True, return_tensors = "pt")
train_encodings["labels"] = train.rubric_idx
train_dataset = Dataset.from_dict(train_encodings)

# Valideerimisandmestik
val_encodings = tokenizer(list(val.text), max_length = 128, padding = "max_length",
                            truncation = True, return_tensors = "pt")
val_encodings["labels"] = val.rubric_idx
val_dataset = Dataset.from_dict(val_encodings)

# Testandmestik
test_encodings = tokenizer(list(test.text), max_length = 128, padding = "max_length",
                            truncation = True, return_tensors = "pt")
test_encodings["labels"] = test.rubric_idx
test_dataset = Dataset.from_dict(test_encodings)

# Jälgimiseks
print("2. Andmestik treenimiseks ette valmistatud")
print(datetime.now())

# Mudelite asukohad, mida kohandatakse
mudelid = ["pretrained_models/checkpoint-100000", "pretrained_models/checkpoint-200000", "pretrained_models/checkpoint-300000", "pretrained_models/checkpoint-400000", "pretrained_models/checkpoint-500000"]

for i in range(len(mudelid)):

    # Mudeli loomine checkpointist
    model = BertForSequenceClassification.from_pretrained(mudelid[i], num_labels=len(rubriik_idx))
    model.to(device)

    # Treeningparameetrite sättimine
    batch_size = 16
    args = TrainingArguments(
        "Rubric_class_results_" + str((i + 1) * 100000),
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
        predictions = np.argmax(predictions, axis=1)

        predictions = [idx_rubriik[pred] for pred in predictions]
        labels = [idx_rubriik[lab] for lab in labels]

        results = classification_report(predictions, labels, output_dict=True)
        return {"precision": results['weighted avg']['precision'], "recall": results['weighted avg']['recall'],
                "f1": results['weighted avg']['f1-score'], "accuracy": results["accuracy"]}

    # Treenija loomine
    trainer = Trainer(
        model,
        args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        compute_metrics=compute_metrics
    )

    # Treenimine
    trainer.train()

    # Testandmetel ennustamine ja täpsuse arvutamine
    predictions, labels, _ = trainer.predict(test_dataset)
    predictions = np.argmax(predictions, axis=1)

    predictions = [idx_rubriik[pred] for pred in predictions]
    labels = [idx_rubriik[lab] for lab in labels]

    results = classification_report(predictions, labels, output_dict=True)
    print(results)

# Jälgimiseks
print("3. Mudelid kohandatud")
print(datetime.now())

### Analoogne kood EstBERT mudeli kohandamiseks

# tokenizer = AutoTokenizer.from_pretrained("tartuNLP/EstBERT")
# train_encodings = tokenizer(list(train.text), max_length = 128, padding = "max_length",
#                             truncation = True, return_tensors = "pt")
# train_encodings["labels"] = train.rubric_idx
# train_dataset = Dataset.from_dict(train_encodings)
#
# val_encodings = tokenizer(list(val.text), max_length = 128, padding = "max_length",
#                             truncation = True, return_tensors = "pt")
# val_encodings["labels"] = val.rubric_idx
# val_dataset = Dataset.from_dict(val_encodings)
#
# test_encodings = tokenizer(list(test.text), max_length = 128, padding = "max_length",
#                             truncation = True, return_tensors = "pt")
# test_encodings["labels"] = test.rubric_idx
# test_dataset = Dataset.from_dict(test_encodings)
#
# model = AutoModelForSequenceClassification.from_pretrained("tartuNLP/EstBERT", num_labels=len(rubriik_idx))
# model.to(device)
#
# batch_size = 16
#
# def compute_metrics(p):
#     predictions, labels = p
#     predictions = np.argmax(predictions, axis=1)
#
#     predictions = [idx_rubriik[pred] for pred in predictions]
#     labels = [idx_rubriik[lab] for lab in labels]
#
#     results = classification_report(predictions, labels, output_dict=True)
#     return {"precision": results['weighted avg']['precision'], "recall": results['weighted avg']['recall'],
#             "f1": results['weighted avg']['f1-score'], "accuracy": results["accuracy"]}
#
#
# args = TrainingArguments(
#     "Rubric_class_results_ESTBERT",
#     evaluation_strategy="epoch",
#     logging_strategy = "epoch",
#     save_strategy = "epoch",
#     learning_rate=5e-5,
#     per_device_train_batch_size=batch_size,
#     per_device_eval_batch_size=batch_size,
#     num_train_epochs=3
# )
#
# trainer = Trainer(
#     model,
#     args,
#     train_dataset=train_dataset,
#     eval_dataset=val_dataset,
#     compute_metrics=compute_metrics
# )
#
# trainer.train()
#
# predictions, labels, _ = trainer.predict(test_dataset)
# predictions = np.argmax(predictions, axis=1)
#
# predictions = [idx_rubriik[pred] for pred in predictions]
# labels = [idx_rubriik[lab] for lab in labels]
#
# results = classification_report(predictions, labels, output_dict=True)
# print(results)

# Jälgimiseks
print("4. Script lõpetatud")
print(datetime.now())
lopp = datetime.now()
print("Kestus")
print(lopp-algus)
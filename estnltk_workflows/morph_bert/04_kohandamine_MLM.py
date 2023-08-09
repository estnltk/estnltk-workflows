# Impordid
import torch
import pandas as pd
import numpy as np
from src.transformers import BertTokenizer, BertForMaskedLM, Trainer, TrainingArguments, DataCollatorForLanguageModeling
from datasets import Dataset
from sklearn.model_selection import train_test_split
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

# 70% train, 10% val, 20% test
train, test = train_test_split(andmed, test_size=0.2)
train, val = train_test_split(train, test_size=0.125)

# Tokeniseerija loomine
tokenizer = BertTokenizer(vocab_file = "vocab_final.txt", vocab_file_form = "vocab_form.txt", max_length = 128,
                         padding = "max_length", truncation = True, return_tensors = "pt", mask_token="ˇMASKˇ")

# Tokeniseeritud andmestike loomine
# Treeningandmestik
train_encodings = tokenizer(list(train.text), max_length = 128, padding = "max_length",
                            truncation = True, return_tensors = "pt")
train_dataset = Dataset.from_dict(train_encodings)

# Valideerimisandmestik
val_encodings = tokenizer(list(val.text), max_length = 128, padding = "max_length",
                            truncation = True, return_tensors = "pt")
val_dataset = Dataset.from_dict(val_encodings)

# Testandmestik
test_encodings = tokenizer(list(test.text), max_length = 128, padding = "max_length",
                            truncation = True, return_tensors = "pt")
test_dataset = Dataset.from_dict(test_encodings)

# Jälgimiseks
print("2. Andmestik treenimiseks ette valmistatud")
print(datetime.now())

# Mudelite asukohad, mida kohandatakse
mudelid = ["pretrained_models/checkpoint-100000", "pretrained_models/checkpoint-200000", "pretrained_models/checkpoint-300000", "pretrained_models/checkpoint-400000", "pretrained_models/checkpoint-500000"]

# Iga mudeli kohta...
for i in range(len(mudelid)):

    # Mudeli loomine checkpointist
    model = BertForMaskedLM.from_pretrained(mudelid[i])
    model.to(device)

    batch_size = 16

    def compute_metrics(p):
        predictions, labels = p
        predictions_lemma = np.argmax(predictions[0], axis=2).ravel() # Ennustuseks on indeks, kus logit on suurim
        labels_lemma = [y[0] for x in labels for y in x]
        predictions_vorm = np.argmax(predictions[1], axis=2).ravel()
        labels_vorm = [y[1] for x in labels for y in x]

        # Arvutame täpsust ainult maskeeritud lemmadel (ehk kus label ei ole -100 -- see tuleb data_collatori vaikeseadest)
        final_pred_lemma = [(p, l) for p, l in zip(predictions_lemma, labels_lemma) if l != -100]
        acc_lemma = sum(np.array([x[0] for x in final_pred_lemma]) == np.array([x[1] for x in final_pred_lemma])) / len(
            final_pred_lemma)

        final_pred_vorm = [(p, l) for p, l in zip(predictions_vorm, labels_vorm) if l != -100]
        acc_vorm = sum(np.array([x[0] for x in final_pred_vorm]) == np.array([x[1] for x in final_pred_vorm])) / len(
            final_pred_vorm)

        return ({'Accuracy_lemma': acc_lemma, 'Accuracy_vorm': acc_vorm, 'n_val': len(final_pred_lemma)})

    # Data collatori loomine
    data_collator = DataCollatorForLanguageModeling(
        tokenizer=tokenizer,
        mlm=True,
        mlm_probability=0.15
    )

    # Treeningparameetrite sättimine (EstBERT põhjal)
    args = TrainingArguments(
        "MLM_results_" + str((i+1)*100000),
        evaluation_strategy="epoch",
        logging_strategy="epoch",
        save_strategy="epoch",
        learning_rate=5e-5,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size,
        num_train_epochs=3
    )

    # Treenija loomine
    trainer = Trainer(
        model,
        args,
        data_collator=data_collator,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        compute_metrics=compute_metrics
    )
    trainer.train()

    # Testandmetel täpsuse leidmine (sama loogika mis compute_metrics funktsioonis)
    predictions, labels, _ = trainer.predict(test_dataset)

    predictions_lemma = np.argmax(predictions[0], axis=2).ravel()
    labels_lemma = [y[0] for x in labels for y in x]
    predictions_vorm = np.argmax(predictions[1], axis=2).ravel()
    labels_vorm = [y[1] for x in labels for y in x]

    final_pred_lemma = [(p, l) for p, l in zip(predictions_lemma, labels_lemma) if l != -100]
    acc_lemma = sum(np.array([x[0] for x in final_pred_lemma]) == np.array([x[1] for x in final_pred_lemma])) / len(final_pred_lemma)

    final_pred_vorm = [(p, l) for p, l in zip(predictions_vorm, labels_vorm) if l != -100]
    acc_vorm = sum(np.array([x[0] for x in final_pred_vorm]) == np.array([x[1] for x in final_pred_vorm])) / len(final_pred_vorm)

    # Testandmestike tulemuste printimine
    print(f"Accuracy lemma : {acc_lemma}")
    print(f"Accuracy vorm : {acc_vorm}")
    print(f"n test : {len(final_pred_lemma)}")

    # Iga mudeli ennustuste väljakirjutamine
    pd.DataFrame(final_pred_lemma).to_csv(str((i+1)*100000) + "test_lemma.csv")
    pd.DataFrame(final_pred_vorm).to_csv(str((i+1)*100000) + "test_vorm.csv")

# Jälgimiseks
print("4. Script lõpetatud")
print(datetime.now())
lopp = datetime.now()
print("Kestus")
print(lopp-algus)
from neo4j import GraphDatabase
from transformers import AutoTokenizer, AutoModel
import torch

class BioBertEmbeddings:
    def __init__(self, tokenizer, model):
        self.tokenizer = tokenizer
        self.model = model

    def embed_query(self, text):
        return self.get_embeddings([text])[0]

    def embed_documents(self, documents)->list:
        return list(self.get_embeddings(documents))

    def get_embeddings(self, texts):
        inputs = self.tokenizer(texts, return_tensors="pt", truncation=True, padding=True, max_length=512)
        with torch.no_grad():
            outputs = self.model(**inputs)

        hidden_states = outputs.last_hidden_state          # (batch, seq_len, hidden)
        mask = inputs["attention_mask"].unsqueeze(-1).float()  # (batch, seq_len, 1)

        summed = (hidden_states * mask).sum(dim=1)
        counts = mask.sum(dim=1).clamp(min=1e-9)
        mean_pooled = summed / counts

        return mean_pooled.numpy()

# Example usage
tokenizer = AutoTokenizer.from_pretrained("dmis-lab/biobert-base-cased-v1.2")
model = AutoModel.from_pretrained("dmis-lab/biobert-base-cased-v1.2")

bio_bert_embeddings = BioBertEmbeddings(tokenizer, model)
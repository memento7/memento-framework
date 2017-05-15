from connection import get_entities

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

entities = get_entities()

filter_quote = lambda x: " ".join(["".join(y) for y in x])

def get_similar(keyword, items):
    entity = " ".join([k for k, v in entities[keyword]['tag'][:100]])
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix_train = tfidf_vectorizer.fit_transform([entity] + items)
    return cosine_similarity(tfidf_matrix_train[0:1], tfidf_matrix_train)[0][1:]
# -------------------------------------------------------------------------
# AUTHOR: Harry Nguyen
# FILENAME: search_engine.py
# SPECIFICATION: This program reads the file collection.csv and output the
# precision/recall of a proposed search engine given the query q = {cat and dogs}
# FOR: CS 4250- Assignment #1
# TIME SPENT: 3 hours
# -----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas.
# You have to work here only with standard arrays

# importing some Python libraries
import csv
import math

documents = []
labels = []

# reading the data in a csv file
with open('collection.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for i, row in enumerate(reader):
        if i > 0:  # skipping the header
            documents.append(row[0])
            labels.append(row[1])

# Conduct stopword removal.
stopWords = {'I', 'and', 'She', 'They', 'her', 'their'}

tokenizedDocs = []
numOfDocs = len(documents)


# Function to remove the stopwords
def rmvStopWords(document):
    doc = document.split()
    for word in doc:
        if word in stopWords:
            doc.remove(word)
    return doc


for i in range(numOfDocs):
    tokenizedDocs.append(rmvStopWords(documents[i]))

# Conduct stemming.
stemming = {
  "cats": "cat",
  "dogs": "dog",
  "loves": "love",
}


# Function to reduce words to their base form
def convertStemWords(tokenized_doc):
    for i in range(len(tokenized_doc)):
        if tokenized_doc[i] in stemming:
            tokenized_doc[i] = stemming[tokenized_doc[i]]
    return tokenized_doc


for i in range(numOfDocs):
    convertStemWords(tokenizedDocs[i])
    print(f'Tokenized d{i+1}: {tokenizedDocs[i]}')

# Identify the index terms.
terms = []
for doc in tokenizedDocs:
    for term in doc:
        if term not in terms:
            terms.append(term)


# Build the if-idf term weights matrix.
# Function to calculate the term frequency in a document
def term_freq(term, document):
    occurrence = len([word for word in document if word == term])
    return occurrence / len(document)


# Function to calculate the Inverse Document Frequency
def inverse_doc_freq(term):
    term_occurrence = 0
    for document in tokenizedDocs:
        if term in document:
            term_occurrence += 1

    # To prevent the case of division by 0
    if term_occurrence == 0:
        term_occurrence = 1

    return math.log10(len(tokenizedDocs)/term_occurrence)


# Function to calculate the tf-idf weight vector of a document
def tf_idf(document):
    tf_idf_vec = []
    for term in terms:
        tf = term_freq(term, document)
        idf = inverse_doc_freq(term)
        weight = tf * idf
        tf_idf_vec.append(round(weight, 4))
    return tf_idf_vec


docMatrix = []
for doc in tokenizedDocs:
    docMatrix.append(tf_idf(doc))

print('\nBelow is the if-idf term weights matrix:')
for term in terms:
    print(f'       {term:>6}', end='')
print('\n', end='')
for i in range(numOfDocs):
    print(f'doc{i + 1}   ', end='')
    for weight in docMatrix[i]:
        print(f'{weight:8.4f}', end='    ')
    print()


# Calculate the document scores (ranking) using document weights (tf-idf) calculated before and query weights
# (binary - have or not the term).
# --> add your Python code here
docScores = []
query = "cat and dogs"
print(f'\nThe user query is "{query}"')
tokenizedQuery = convertStemWords(rmvStopWords(query))
print('Tokenized query is ', tokenizedQuery)

binary_query = [1 if term in tokenizedQuery else 0 for term in terms]
print(f'The binary query is {binary_query}\n')


# Function to calculate the similarity between two vectors for ranking
def cosine_similarity(vector1, vector2):
    dot_product = sum(x * y for x, y in zip(vector1, vector2))
    magnitude1 = math.sqrt(sum(x**2 for x in vector1))
    magnitude2 = math.sqrt(sum(x**2 for x in vector2))

    # Handle division by 0
    if magnitude1 == 0 or magnitude2 == 0:
        return 1

    return dot_product/(magnitude1*magnitude2)


# Calculate the score of each document compared with the binary user query
for doc_vector in docMatrix:
    score = cosine_similarity(binary_query, doc_vector)
    docScores.append(round(score, 4))

for i in range(len(docScores)):
    print(f'Document {i+1} score is {docScores[i]:.4f}')

# Calculate the precision and recall of the model by considering that the search engine will return all documents
# with weights >= 0.1.
retrieved = set()
for i in range(len(docScores)):
    if docScores[i] >= 0.1:
        retrieved.add(i+1)
print('\nRetrieved documents: ', retrieved)

# Based on the ground truth in the csv file to get the relevant documents
relevant = set()
for i in range(len(labels)):
    if labels[i].strip() == 'R':
        relevant.add(i+1)
print('Relevant documents: ', relevant)

hits = retrieved.intersection(relevant)
misses = relevant.difference(hits)
noise = retrieved.difference(relevant)
print('Hit documents: ', hits)
print('Missed documents: ', misses)
print('Noise: ', noise)
print(f'\nSo hits = {len(hits)}, misses = {len(misses)}, and noise = {len(noise)}')

# Calculate the recall and precision of the model
recall = len(hits) / (len(hits) + len(misses)) * 100
precision = len(hits) / (len(hits) + len(noise)) * 100
print(f'\nThe precision: {precision:.2f}%\nThe recall: {recall:.2f}%')

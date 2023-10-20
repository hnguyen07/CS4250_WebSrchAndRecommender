# -------------------------------------------------------------------------
# AUTHOR: Harry Nguyen
# FILENAME: db_connection_mongo_solution.py
# SPECIFICATION: Connect to a database and manipulate the NoSQL database
# FOR: CS 4250- Assignment #2
# TIME SPENT: 3 hours
# -----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas.
# You have to work here only with standard arrays

# importing some Python libraries
from pymongo import MongoClient
import datetime


def connectDataBase():

    # Create a database connection object using pymongo
    DB_HOST = 'localhost:27017'

    try:
        # Creating an instance of MongoClient and informing the connection string
        client = MongoClient(host=[DB_HOST])
        # Creating database
        db = client.corpusMongo

        return db

    except:
        print("Database not connected successfully")


def createDocument(documents, docId, docText, docTitle, docDate, docCat):
    # 1 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    docText = docText.strip()
    docTextNoPunc = docText
    punc = '!;:,.?'  # Punctuation mark set based on the provided database - can be expanded further if needed

    for ele in docTextNoPunc:
        if ele in punc:
            docTextNoPunc = docTextNoPunc.replace(ele, '')  # Remove punctuation marks

    num_chars = len(docTextNoPunc.replace(' ', ''))  # Remove spaces

    # 3 Get a list of terms along with their occurrences in the docText .
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and
    # Remember to lowercase terms and remove punctuation marks.
    docTerms = docTextNoPunc.lower().split()

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure to stores how many times (count) each term appears in the document

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    term_freq = {}
    for term in docTerms:
        # term_freq.get(term, 0): get the current frequency of the term in the doc.
        # If the term is not in the dictionary, it defaults to 0.
        term_freq[term] = term_freq.get(term, 0) + 1

    # create a list of dictionaries to include term objects.
    terms = []
    for term in term_freq.keys():
        termDict = dict()
        termDict["term"] = term
        termDict["count"] = term_freq[term]
        termDict["num_chars"] = len(term)
        terms.append(termDict)

    # Produce a final document as a dictionary including all the required document fields
    document = dict()
    document["_id"] = docId
    document["title"] = docTitle
    document["text"] = docText
    document["num_chars"] = num_chars
    document["date"] = datetime.datetime.strptime("{}T00:00:00.000Z".format(docDate),
                                                  "%Y-%m-%dT%H:%M:%S.000Z")
    document["category"] = docCat
    document["terms"] = terms

    # Insert the document
    documents.insert_one(document)


def deleteDocument(documents, docId):

    # Delete the document from the database
    documents.delete_one({"_id": docId})


def updateDocument(documents, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(documents, docId)

    # 2 Create the document with the same id
    createDocument(documents, docId, docText, docTitle, docDate, docCat)


def getIndex(documents):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...

    # Perform aggregation operations
    pipeline = [
        {"$unwind": "$terms"},
        {"$project": {"_id": 0, "title": 1, "terms.term": 1, "terms.count": 1}},
        {"$sort": {"terms.term": 1}}
    ]
    docs = documents.aggregate(pipeline)

    inverted_indexes = {}
    for doc in docs:
        index = dict()
        index[doc["title"]] = doc["terms"]["count"]

        if doc["terms"]["term"] in inverted_indexes:
            inverted_indexes[doc["terms"]["term"]].append(index)
        else:
            inverted_indexes[doc["terms"]["term"]] = [index]

    # Use the inverted_indexes dict to create a desired result string to output to the screen
    result = ''
    for term, indexes in inverted_indexes.items():
        result += "'{}': '".format(term)
        # Get the title of the document and the frequency of the term in that document
        for idx in indexes:
            for doc, count in idx.items():
                result += "{}:{}, ".format(doc, count)
        # Remove ", " behind the last element and add "', "
        result = result[:-2] + "', "

    # Remove ", " behind the last element and surround the index result with "{}"
    result = '{' + result[:-2] + '}'

    return result

# -------------------------------------------------------------------------
# AUTHOR: Harry Nguyen
# FILENAME: db_connection_solution.py
# SPECIFICATION: Connect to a database and manipulate the related relation tables (SQL)
# FOR: CS 4250- Assignment #2
# TIME SPENT: 3 hours
# -----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas.
# You have to work here only with standard arrays

# importing some Python libraries
import psycopg2
from psycopg2.extras import RealDictCursor


def connectDataBase():

    # Create a database connection object using psycopg2
    DB_NAME = "corpus"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn

    except:
        print("Database not connected successfully")


def createCategory(cur, catId, catName):

    # Insert a category in the database
    sql = "Insert into categories (id, name) Values (%s, %s)"
    recset = [catId, catName]
    cur.execute(sql, recset)


def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    cur.execute("select * from categories where name like %(docCat)s",
                {'docCat': '%{}%'.format(docCat)})
    recset = cur.fetchall()

    if recset:
        catId = recset[0]['id']
    else:
        return []

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    docText = docText.strip()
    docTextNoPunc = docText
    punc = '!;:,.?'  # Punctuation mark set based on the provided database - can be expanded further if needed

    for ele in docTextNoPunc:
        if ele in punc:
            docTextNoPunc = docTextNoPunc.replace(ele, '')  # Remove punctuation marks

    num_chars = len(docTextNoPunc.replace(' ', ''))  # Remove spaces

    sql = "Insert into documents (doc_number, text, title, num_chars, date, category_id) " \
          "Values (%s, %s, %s, %s, %s, %s)"
    recset = [docId, docText, docTitle, num_chars, docDate, catId]
    cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and
    # Remember to lowercase terms and remove punctuation marks.
    docTerms = docTextNoPunc.lower().split()

    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    for term in docTerms:
        cur.execute("select * from terms where term like %(term)s",
                    {'term': term})

        recset = cur.fetchall()
        if not recset:  # the term does not exist
            sql = "Insert into terms (term, num_chars) Values (%s, %s)"
            recset = [term, len(term)]
            cur.execute(sql, recset)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure to stores how many times (count) each term appears in the document
    term_freq = {}  # dictionary is the most suitable data structure

    for term in docTerms:
        # term_freq.get(term, 0): get the current frequency of the term.
        # If the term is not in the dictionary, it defaults to 0.
        term_freq[term] = term_freq.get(term, 0) + 1

    # 4.3 Insert the term and its corresponding count into the database
    sql = "Insert into indexes (term, doc_number, count) " \
          "Values (%s, %s, %s)"

    # Insert each term and its frequency to the database
    for term in term_freq.keys():
        recset = [term, docId, term_freq[term]]
        cur.execute(sql, recset)


def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    cur.execute("select * from indexes where doc_number = %(doc_number)s",
                {'doc_number': docId})

    recset = cur.fetchall()

    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document.
    # If this happens, delete the term from the database.
    for rec in recset:
        # delete its occurrences in the index for that document
        cur.execute("Delete from indexes where term = %s and doc_number = %s",
                    [rec['term'], docId])
        # Check if there are no more occurrences of the term in another document
        cur.execute("select * from indexes where term = %(term)s",
                    {'term': rec['term']})

        recs = cur.fetchall()

        # If there are no more occurrences of the term in another document
        if not recs:
            # Delete the term from the database
            cur.execute("Delete from terms where term = %(term)s",
                        {'term': rec['term']})

    # 2 Delete the document from the database
    cur.execute("Delete from documents where doc_number = %(doc_number)s",
                {'doc_number': docId})


def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)


def getIndex(cur):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    cur.execute("select term from terms order by term")
    recset = cur.fetchall()

    terms = []
    for rec in recset:
        terms.append(rec['term'])

    index = ''  # to store terms along with their frequency in each documents in the database
    for term in terms:
        cur.execute("select * from indexes inner join documents on indexes.doc_number = documents.doc_number"
                    " where term = %(term)s order by indexes.doc_number",
                    {'term': term})

        recset = cur.fetchall()

        index += "'{}': '".format(term)

        for rec in recset:
            # Get the title of the document and the frequency of the term in that document
            index += "{}:{}, ".format(rec['title'], rec['count'])

        # Remove ", " behind the last element and add "', "
        index = index[:-2] + "', "

    # Remove ", " behind the last element and surround the index result with "{}"
    index = '{' + index[:-2] + '}'

    return index

# -------------------------------------------------------------------------
# AUTHOR: Harry Nguyen
# FILENAME: parser.py
# SPECIFICATION: Read the CS Faculty information, parse faculty members name, title, office, email, and website, and
#                persist this data in MongoDB collection.
# FOR: CS 4250- Assignment #3
# TIME SPENT: 2 hours
# -----------------------------------------------------------*/

# importing some Python libraries
from bs4 import BeautifulSoup
from pymongo import MongoClient
import regex as re


def connectDataBase():
    # Create a database connection object using pymongo
    DB_NAME = 'cs_department'
    DB_HOST = 'localhost'
    DB_PORT = 27017

    try:
        # Create an instance of MongoClient
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        # Create a database
        db = client[DB_NAME]
        return db
    except:
        print('Database not connected successfully')


def main():
    # Connect to the database
    db = connectDataBase()
    pages = db.pages
    # Create a collection to store the professors' info
    professors = db.professors

    query = {'url': 'https://www.cpp.edu/sci/computer-science/faculty-and-staff/permanent-faculty.shtml'}
    # Retrieve the record of the Permanent Faculty page from the database based on the provided URL
    result = pages.find_one(query)

    # Check if the HTML content was found
    if not result:
        print('No matching records found')
    else:
        # Get the HTML content of the URL
        html = result['html']
        print('HTML content of CS Permanent Faculty extracted successfully!')
        bs = BeautifulSoup(html, 'html.parser')

        # Create lists for each data field
        prof_names = []
        prof_titles = []
        prof_offices = []
        prof_emails = []
        prof_webs = []

        # Get the professors' names
        name_list = bs.find_all('h2')
        for name in name_list:
            prof_names.append(name.get_text().strip())
        prof_names = prof_names[1:]  # Exclude the Permanent Faculty text

        # Get the professors' titles
        title_element = bs.find_all('strong', string=re.compile('Title:?'))
        for title in title_element:
            # Get the next sibling, which is the text containing the title
            prof_title = title.find_next_sibling(string=True).strip()
            # Remove noise from the data
            if prof_title[0] == ':':
                prof_titles.append(prof_title[2:])
            else:
                prof_titles.append(prof_title)

            # Get the professors' office numbers (next to title is office number)
            # Get the next sibling, which is the text containing the office number
            office_element = title.find_next('strong')
            office = office_element.find_next_sibling(string=True).strip()
            # Remove noise from the data
            if office[0] == ':':
                prof_offices.append(office[2:])
            else:
                prof_offices.append(office)

            # Get the professors' emails (next to office number is phone, next to phone is email)
            # Have to traverse two times from office section to get to email section
            email_element = office_element.find_next('strong').find_next('strong')
            prof_emails.append(email_element.find_next('a')['href'].replace('mailto:', ''))

            # Get the professors' websites (next to email is website)
            web_element = email_element.find_next('strong')
            web = web_element.find_next('a')['href']
            # Remove noise from the data and make the websites' format uniform
            if web.find('index.shtml') != -1:
                prof_webs.append(web.replace('index.shtml', ''))
            else:
                if re.search('/$', web):
                    prof_webs.append(web)
                else:
                    prof_webs.append(web + '/')

        counter = 1
        for name, title, office, email, website in zip(prof_names, prof_titles, prof_offices, prof_emails, prof_webs):
            # Create a record of a professor with the required fields including name, title, office, email, and website
            professor = dict()
            professor['_id'] = counter
            professor['name'] = name
            professor['title'] = title
            professor['office'] = office
            professor['email'] = email
            professor['website'] = website

            # Print out the professor's record to have a general view of the data
            print(professor)

            # Insert the professor record to the collection
            professors.insert_one(professor)
            # Increment the id number to be used in the database
            counter += 1


if __name__ == '__main__':
    main()

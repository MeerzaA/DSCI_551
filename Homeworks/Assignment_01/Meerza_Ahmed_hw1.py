# IMPORT LIBRARIES
import json
import requests
import sys


# Firebase Database URLs (Replace these URLs with your actual Firebase URLs)
DATABASE_URLS = {
    0: "",
    1: ""
}


# Define any global methods here
response0 = requests.get(DATABASE_URLS[0] + ".json")
book0 = json.loads(response0.text)

response1 = requests.get(DATABASE_URLS[1] + ".json")
book1 = json.loads(response1.text)


def hash_func(Djson):

    json_data = json.loads(Djson)
    
    if len(json_data["author"]) % 2 == 0:
        num = 0
    else:
        num = 1
    return num


# python .\Meerza_Ahmed_hw1.py add_book 104 '{\"author\": \"John Smith\", \"title\": \"Atomic Habits\", \"year\": 2023, \"price\": \"60\"}' | for VScode windows
# https://dsci551-spr24-default-rtdb.firebaseio.com/ + DATABASE_URLS[x] + book_id + book_json + .json
def add_book(book_id, book_json):
    
    # INPUT : book id and book json from command line
    id = book_id
    data = json.loads(book_json)
    database_num = hash_func(book_json)

    # RETURN : status code after pyhton REST call to add book [response.status_code]
    response = requests.put(DATABASE_URLS[database_num] + "/" + id + ".json", json=data)
    
    # EXPECTED RETURN : 200
    return response

# python .\Meerza_Ahmed_hw1.py search_by_author "John Smith" | for VScode windows
def search_by_author(author_name):
    
    # INPUT: Name of the author
    arut_seach = author_name
    match_list = {}
    
    for book_keys, book_values in book0.items():
        if book_values['author'] == arut_seach:
            match = book0.get(book_keys)
            match_list[book_keys] = match 
    
    
    for book_keys, book_values in book1.items():
        if book_values['author'] == arut_seach:
            match = book1.get(book_keys)
            match_list[book_keys] = match 
    
    # RETURN: JSON object having book_ids as keys and book information as value [book_json] published by that author  
    matches = json.dumps(match_list)
    
    # EXPECTED RETURN TYPE: {'102': {'author': ... , 'price': ..., 'title': ..., 'year': ...}, '104': {'author': , 'price': , 'title': , 'year': }}
    return matches


# python .\Meerza_Ahmed_hw1.py search_by_year 2023 | for VScode windows
def search_by_year(year):

    # INPUT: Year when the book published
    search_yr = year
    match_list = {}
    
    for book_keys, book_values in book0.items():
        if book_values['year'] == search_yr:
            match = book0.get(book_keys)
            match_list[book_keys] = match 
        
        
    for book_keys, book_values in book1.items():
        if book_values['year'] == search_yr:
            match = book1.get(book_keys)
            match_list[book_keys] = match 
 
    # RETURN: JSON object having book_ids as key and book information as value [book_json] published in that year
    matches = json.dumps(match_list)
    
    # EXPECTED RETURN TYPE: {'102': {'author': ... , 'price': ..., 'title': ..., 'year': ...}, '104': {'author': , 'price': , 'title': , 'year': }}
    return matches

# Use the below main method to test your code
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py [operation] [arguments]")
        
    operation = sys.argv[1].lower()
    if operation == "add_book":
        result = add_book(sys.argv[2], sys.argv[3])
        print(result)
    elif operation == "search_by_author":
        books = search_by_author(sys.argv[2])
        print(books)
    elif operation == "search_by_year":
        year = int(sys.argv[2])
        books = search_by_year(year)
        print(books)

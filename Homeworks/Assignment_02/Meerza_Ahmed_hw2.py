# IMPORT LIBRARIES
import sys
import json
from lxml import etree

# Define any helper functions here
def hash_func(Djson):

    json_data = json.loads(Djson)
    
    if len(json_data["author"]) % 2 == 0:
        num = 0
    else:
        num = 1
    return num

def printf(elems):
    if (isinstance(elems, list)):
        for elem in elems:
            if isinstance(elem, str):
                print(elem)
            else:
                print(etree.tostring(elem).decode('utf-8'))
    else: # just a single element
        print(etree.tostring(elems).decode('utf-8'))

#python3 ./Meerza_Ahmed_hw2.py file0.xml file1.xml add_book 104 '{\"author\": \"John Smith\", \"title\": \"Atomic Habits\", \"year\": 2023, \"price\": \"60\"}' | for VScode windows
def add_book(book_id, book_json):
    
    # INPUT : book json from command line
    result = 0
    num = hash_func(book_json)
    print(num)
    json_data = json.loads(book_json)
    
    tree = etree.parse(XML_FILES[num])
    root = tree.getroot()

    new_book = etree.Element("book", id="{}".format(book_id))
    
    auth = etree.SubElement(new_book, "author")
    auth.text = json_data.get("author")
    
    tit = etree.SubElement(new_book, "title")
    tit.text = json_data.get("title")
    
    yer = etree.SubElement(new_book, "year")
    yer.text = str(json_data.get("year"))

    pric = etree.SubElement(new_book, "price")
    pric.text = str(json_data.get("price"))


    # RETURN : 1 if successful, else 0 
    dupe = []
    chk = tree.xpath('/bib/book/@id')
    print(chk)
    
    for x in chk:
        dupe.append(x)

    
    if  book_id in dupe:
        result = 0
    else:
        root.append(new_book)
        tree.write(XML_FILES[num])
        
        result = 1
    
    # Assume JSON is well formed with no missing attributes
    return result

#python3 ./Meerza_Ahmed_hw2.py file0.xml file1.xml search_by_author "John Smith" | for VScode windows
def search_by_author(author_name):
    
    # INPUT: name of author
    tree0 = etree.parse(xml0)
    tree1 = etree.parse(xml1)

    searchStr0 = '/bib/book[author="{}"]/title'.format(author_name)
    searchStr1 = '/bib/book[author="{}"]/title'.format(author_name)
    
    matches0 = tree0.xpath(searchStr0)
    matches1 = tree1.xpath(searchStr1)

    # RETURN: list of strings containing only book titles
    results = []

    for x in matches0:
        results.append(x.text)

    for x in matches1:
        results.append(x.text)

    # EXPECTED RETURN TYPE: ['book title 1', 'book title 2', ...]
    return results

#python3 ./Meerza_Ahmed_hw2.py file0.xml file1.xml search_by_year 2023 | for VScode windows
def search_by_year(year):
    
    # INPUT: year of publication
    tree0 = etree.parse(xml0)
    tree1 = etree.parse(xml1)

    searchStr0 = '/bib/book[year={}]/title'.format(year)
    searchStr1 = '/bib/book[year={}]/title'.format(year)
    
    matches0 = tree0.xpath(searchStr0)
    matches1 = tree1.xpath(searchStr1)
    
    # RETURN: list of strings containing only book titles
    results = []

    for x in matches0:
        results.append(x.text)

    for x in matches1:
        results.append(x.text)

    # EXPECTED RETURN TYPE: ['book name 1', 'book name 2', ...]
    return results

# Use the below main method to test your code
if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.exit("\nUsage: python3 script.py [path/to/file0.xml] [path/to/file1.xml] [operation] [arguments]\n")

    xml0, xml1 = sys.argv[1], sys.argv[2]

    # Assume XML files exist at mentioned path and are initialized with empty <bib> </bib> tags
    global XML_FILES 
    XML_FILES = {
        0: xml0,
        1: xml1
    }

    operation = sys.argv[3].lower()

    if operation == "add_book":
        result = add_book(sys.argv[4], sys.argv[5])
        print(result)
    elif operation == "search_by_author":
        books = search_by_author(sys.argv[4])
        print(books)
    elif operation == "search_by_year":
        year = int(sys.argv[4])
        books = search_by_year(year)
        print(books)
    else:
        sys.exit("\nInvalid operation.\n")

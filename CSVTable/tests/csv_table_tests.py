"""

csv_table_tests.py

"""

from src.CSVDataTable import CSVDataTable

import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
data_dir = os.path.abspath("../Data/Baseball")


def tests_people():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    people = CSVDataTable("People", connect_info, ["playerID"])
    try:

        print()
        print("find_by_primary_key(): Known Record")
        print(people.find_by_primary_key(["aardsda01"]))

        print()
        print("find_by_primary_key(): Unknown Record")
        print(people.find_by_primary_key((["cah2251"])))

        print()
        print("find_by_template(): Known Template")
        template = {"nameFirst": "David", "nameLast": "Aardsma", "nameGiven": "David Allan"}
        print(people.find_by_template(template))

        print()
        print("insert(): New Record")
        new_record = {'playerID': 'yy3089', 'birthYear': '1998', 'birthMonth': '4', 'birthDay': '14',
                      'birthCountry': 'PRC', 'birthState': '', 'birthCity': 'Nanjing', 'deathYear': '',
                      'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '',
                      'nameFirst': 'Yi', 'nameLast': '', 'nameGiven': 'Yang', 'weight': '215', 'height': '75',
                      'bats': 'R', 'throws': 'R', 'debut': '', 'finalGame': '', 'retroID': 'yy3089',
                      'bbrefID': 'yy3089'}
        print(people.insert(new_record))
        print("find_by_primary_key(): Known Record")
        print(people.find_by_primary_key(["yy3089"]))

        print()
        print('update_by_key(): Known Record')
        print("Update %d record(s)" % people.update_by_key(["yy3089"], {'debut': '2021-9-9'}))
        print("find_by_primary_key(): Known Record")
        print(people.find_by_primary_key(["yy3089"]))

        print()
        print('update_by_template(): Known Template')
        print("Update %d record(s)" % people.update_by_template({'playerID': 'yy3089'}, {'finalGame': '2021-9-21'}))
        print("find_by_primary_key(): Known Record")
        print(people.find_by_primary_key(["yy3089"]))

        print()
        print("delete_by_key(): key_fields")
        print("Deleted %d record(s)" % people.delete_by_key(["yy3089"]))

        print()
        print("insert(): New Record")
        new_record = {'playerID': 'yy3089', 'birthYear': '1998', 'birthMonth': '4', 'birthDay': '14',
                      'birthCountry': 'PRC', 'birthState': '', 'birthCity': 'Nanjing', 'deathYear': '',
                      'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '',
                      'nameFirst': 'Yi', 'nameLast': '', 'nameGiven': 'Yang', 'weight': '215', 'height': '75',
                      'bats': 'R', 'throws': 'R', 'debut': '', 'finalGame': '', 'retroID': 'yy3089',
                      'bbrefID': 'yy3089'}
        print(people.insert(new_record))
        print("find_by_template(): Known Template")
        print(people.find_by_primary_key(["yy3089"]))

        print()
        print("delete_by_template(): template")
        template = {'birthCity': 'Nanjing'}
        print("Deleted %d record(s)" % people.delete_by_template(template))

        # Robustness for Duplicate Primary Key

        print()
        print('update_by_key(): Duplicate Primary Key')
        print("Update %d record(s)" % people.update_by_key(["aardsda01"], {'playerID': 'aberal01'}))

        print()
        print('update_by_template(): Duplicate Primary Key')
        print("Update %d record(s)" % people.update_by_template({'playerID': 'aardsda01'}, {'playerID': 'aberal01'}))

        print()
        print('insert(): Duplicate Primary Key')
        print(people.insert({'playerID': 'aberal01', 'birthYear': '1998', 'birthMonth': '4', 'birthDay': '14',
                             'birthCountry': 'PRC', 'birthState': '', 'birthCity': 'Nanjing', 'deathYear': '',
                             'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '',
                             'nameFirst': 'Yi', 'nameLast': '', 'nameGiven': 'Yang', 'weight': '215', 'height': '75',
                             'bats': 'R', 'throws': 'R', 'debut': '', 'finalGame': '', 'retroID': 'yy3089',
                             'bbrefID': 'yy3089'}))

        # Please complete code IN THE SAME FORMAT to test when the rest of methods pass or fail
        # HINT HINT: Don't forget about testing the primary key integrity constraints!!
        # For these tests, think to yourself: When should this fail? When should this pass?

    except Exception as e:
        print("An error occurred:", e)


def tests_batting():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    batting = CSVDataTable("Batting", connect_info, ["playerID", "yearID", "stint"])

    try:
        print()
        print("find_by_primary_key(): Known Record")
        print(batting.find_by_primary_key(["abercda01", "1871", "1"]))

        print()
        print("find_by_primary_key(): Unknown Record")
        print(batting.find_by_primary_key((["cah2251", "2251", "1"])))

        print()
        print("find_by_template(): Known Template")
        template = {"playerID": "abercda01", "yearID": "1871", "stint": "1"}
        print(batting.find_by_template(template))

        print()
        print("insert(): New Record")
        new_record = {'playerID': 'yy3089', 'yearID': '1998', 'stint': '1', 'teamID': 'TRO', 'lgID': 'NA', 'G': '1',
                      'AB': "4", 'R': '0', 'H': '0', '2B': '0', '3B': '0', 'HR': '0', 'RBI': '0', 'SB': '0', 'CS': '0',
                      'BB': '0', 'SO': '0', 'IBB': '', 'HBP': '', 'SH': '', 'SF': '', 'GIDP': '0'}
        print(batting.insert(new_record))
        print("find_by_primary_key(): Known Record")
        print(batting.find_by_primary_key(["yy3089", "1998", "1"]))

        print()
        print('update_by_key(): Known Record')
        print("Update %d record(s)" % batting.update_by_key(["yy3089", "1998", "1"], {'teamID': 'FCB'}))
        print("find_by_primary_key(): Known Record")
        print(batting.find_by_primary_key(["yy3089", "1998", "1"]))

        print()
        print('update_by_template(): Known Template')
        print("Update %d record(s)" % batting.update_by_template({'playerID': 'yy3089', 'yearID': '1998', 'stint': '1'},
                                                                {'teamID': 'RMA'}))
        print("find_by_primary_key(): Known Record")
        print(batting.find_by_primary_key(["yy3089", "1998", "1"]))

        print()
        print("delete_by_key(): key_fields")
        print("Deleted %d record(s)" % batting.delete_by_key(["yy3089", "1998", "1"]))

        print()
        print("insert(): New Record")
        new_record = {'playerID': 'yy3089', 'yearID': '1998', 'stint': '1', 'teamID': 'RMA', 'lgID': 'NA', 'G': '1',
                      'AB': "4", 'R': '0', 'H': '0', '2B': '0', '3B': '0', 'HR': '0', 'RBI': '0', 'SB': '0', 'CS': '0',
                      'BB': '0', 'SO': '0', 'IBB': '', 'HBP': '', 'SH': '', 'SF': '', 'GIDP': '0'}
        print(batting.insert(new_record))
        print("find_by_primary_key(): Known Record")
        print(batting.find_by_primary_key(["yy3089", "1998", "1"]))

        print()
        print("delete_by_template(): template")
        template = {'teamID': 'RMA'}
        print("Deleted %d record(s)" % batting.delete_by_template(template))

        # Robustness for Duplicate Primary Key

        print()
        print('update_by_key(): Duplicate Primary Key')
        print("Update %d record(s)" % batting.update_by_key(["abercda01", "1871", "1"],
                                                            {'playerID': 'addybo01', 'yearID': '1871', 'stint': '1'}))

        print()
        print('update_by_template(): Duplicate Primary Key')
        print("Update %d record(s)" % batting.update_by_template({'playerID': 'aardsda01', 'yearID': '1871', 'stint': '1'},
                                                                 {'playerID': 'addybo01', 'yearID': '1871', 'stint': '1'}))

        print()
        print('insert(): Duplicate Primary Key')
        print(batting.insert({'playerID': 'addybo01', 'yearID': '1871', 'stint': '1', 'teamID': 'RMA', 'lgID': 'NA',
                              'G': '1', 'AB': "4", 'R': '0', 'H': '0', '2B': '0', '3B': '0', 'HR': '0', 'RBI': '0',
                              'SB': '0', 'CS': '0', 'BB': '0', 'SO': '0', 'IBB': '', 'HBP': '', 'SH': '', 'SF': '',
                              'GIDP': '0'}))

    except Exception as e:
        print("An error occurred:", e)

tests_people()
tests_batting()

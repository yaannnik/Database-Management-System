import CSVTable
import CSVCatalog
import json
import csv

#Must clear out all tables in CSV Catalog schema before using if there are any present
#Please change the path name to be whatever the path to the CSV files are
#First methods set up metadata!! Very important that all of these be run properly

# Only need to run these if you made the tables already in your CSV Catalog tests
# You will not need to include the output in your submission as executing this is not required
# Implementation is provided
def drop_tables_for_prep():
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("appearances")

#drop_tables_for_prep()

# Implementation is provided
# You will need to update these with the correct path
def create_lahman_tables():
    cat = CSVCatalog.CSVCatalog()
    cat.create_table("people", "/Users/arapeterson/Desktop/Databases/HW2/People.csv")
    cat.create_table("batting","/Users/arapeterson/Desktop/Databases/HW2/Batting.csv")
    cat.create_table("appearances", "/Users/arapeterson/Desktop/Databases/HW2/Appearances.csv")

#create_lahman_tables()

# Note: You can default all column types to text
def update_people_columns():
    # ************************ TO DO ***************************
    pass

#update_people_columns()

def update_appearances_columns():
    # ************************ TO DO ***************************
    pass

#update_appearances_columns()

def update_batting_columns():
    # ************************ TO DO ***************************
    pass

#update_batting_columns()

#Add primary key indexes for people, batting, and appearances in this test
def add_index_definitions():
    # ************************ TO DO ***************************
    pass

#add_index_definitions()


def test_load_info():
    table = CSVTable.CSVTable("people")
    print(table.__description__.file_name)

#test_load_info()

def test_get_col_names():
    table = CSVTable.CSVTable("people")
    names = table.__get_column_names__()
    print(names)

#test_get_col_names()

def add_other_indexes():
    """
    We want to add indexes for common user stories
    People: nameLast, nameFirst
    Batting: teamID
    Appearances: None that are too important right now
    :return:
    """
    # ************************ TO DO ***************************
    pass

#add_other_indexes()

def load_test():
    batting_table = CSVTable.CSVTable("batting")
    print(batting_table)

#load_test()


def dumb_join_test():
    batting_table = CSVTable.CSVTable("batting")
    appearances_table = CSVTable.CSVTable("appearances")
    result = batting_table.dumb_join(appearances_table, ["playerID", "yearID"], {"playerID": "baxtemi01"},
                                     ["playerID", "yearID", "teamID", "AB", "H", "G_all", "G_batting"])
    print(result)


#dumb_join_test()


def get_access_path_test():
    batting_table = CSVTable.CSVTable("batting")
    template = ["teamID", "playerID", "yearID"]
    index_result, count = batting_table.__get_access_path__(template)
    print(index_result)
    print(count)

#get_access_path_test()

def sub_where_template_test():
    # ************************ TO DO ***************************
    pass

#sub_where_template_test()


def test_find_by_template_index():
    # ************************ TO DO ***************************
    pass

#test_find_by_template_index()

def smart_join_test():
    # ************************ TO DO ***************************
    pass

#smart_join_test()

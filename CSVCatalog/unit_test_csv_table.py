import CSVTable
import CSVCatalog
import json
import csv
import pandas as pd
import pymysql


# Must clear out all tables in CSV Catalog schema before using if there are any present
# Please change the path name to be whatever the path to the CSV files are
# First methods set up metadata!! Very important that all of these be run properly

# A function to reset the database to a default state for testing.
def reset_db():
    # TODO: My changes here
    conn = pymysql.connect(host="localhost",
                           port=3306,
                           user="root",
                           password="dbuserdbuser",
                           db="CSVCatalog")

    q = "delete from csvindexes"
    res = CSVCatalog.run_q(conn, q, args=None, fetch=True)
    q = "delete from csvcolumns"
    res = CSVCatalog.run_q(conn, q, args=None, fetch=True)
    q = "delete from csvtables"
    res = CSVCatalog.run_q(conn, q, args=None, fetch=True)

# Only need to run these if you made the tables already in your CSV Catalog tests
# You will not need to include the output in your submission as executing this is not required
# Implementation is provided
def drop_tables_for_prep():
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("appearances")


# drop_tables_for_prep()

# Implementation is provided
# You will need to update these with the correct path
def create_lahman_tables():
    cat = CSVCatalog.CSVCatalog()

    columns = list(pd.read_csv("./People.csv").columns)
    column_definitions = []
    for column in columns:
        column_definitions.append(CSVCatalog.ColumnDefinition(column))
    cat.create_table("people", "./People.csv", column_definitions)

    columns = list(pd.read_csv("./Batting.csv").columns)
    column_definitions = []
    for column in columns:
        column_definitions.append(CSVCatalog.ColumnDefinition(column))
    cat.create_table("batting", "./Batting.csv", column_definitions)

    columns = list(pd.read_csv("./Appearances.csv").columns)
    column_definitions = []
    for column in columns:
        column_definitions.append(CSVCatalog.ColumnDefinition(column))
    cat.create_table("appearances", "./Appearances.csv", column_definitions)


# create_lahman_tables()

# Note: You can default all column types to text
def update_people_columns():
    # TODO: My changes here
    tab = CSVTable.CSVTable("people")
    template = {"playerID": "aardsda01", "retroID": "aardd001", "bbrefID": "aardsda"}
    new_value = {"weight": "216"}
    tab.update(template, new_value)


# update_people_columns()

def update_appearances_columns():
    # TODO: My changes here
    tab = CSVTable.CSVTable("appearances")
    template = {"playerID": "abercda01", "yearID": "1871", "teamID": "TRO", "lgID": "NA"}
    new_value = {"G_all": "2"}
    tab.update(template, new_value)


# update_appearances_columns()

def update_batting_columns():
    # TODO: My changes here
    tab = CSVTable.CSVTable("batting")
    template = {"playerID": "abercda01", "yearID": "1871", "teamID": "TRO", "lgID": "NA"}
    new_value = {"G": "2"}
    tab.update(template, new_value)


# update_batting_columns()

# Add primary key indexes for people, batting, and appearances in this test
def add_index_definitions():
    # TODO: My changes here
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("people")
    t.define_index("primary_index", ["playerID"], "PRIMARY")

    t = cat.get_table("batting")
    t.define_index("primary_index", ["playerID"], "PRIMARY")

    t = cat.get_table("appearances")
    t.define_index("primary_index", ["playerID"], "PRIMARY")


# add_index_definitions()


def test_load_info():
    table = CSVTable.CSVTable("people")
    print(table.__description__.file_name)


# test_load_info()

def test_get_col_names():
    table = CSVTable.CSVTable("people")
    names = table.__get_column_names__()
    print(names)


# test_get_col_names()

def add_other_indexes():
    """
    We want to add indexes for common user stories
    People: nameLast, nameFirst
    Batting: teamID
    Appearances: None that are too important right now
    :return:
    """
    # TODO: My changes here
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("people")
    t.define_index("name_index", ["nameLast", "nameFirst"], "INDEX")

    t = cat.get_table("appearances")
    t.define_index("team_index", ["teamID"], "INDEX")


# add_other_indexes()

def load_test():
    batting_table = CSVTable.CSVTable("batting")
    print(batting_table)


# load_test()


def dumb_join_test():
    batting_table = CSVTable.CSVTable("batting")
    appearances_table = CSVTable.CSVTable("appearances")
    result = batting_table.dumb_join(appearances_table, ["playerID", "yearID"], {"playerID": "baxtemi01"},
                                     ["playerID", "yearID", "teamID", "AB", "H", "G_all", "G_batting"])
    print(result)


# dumb_join_test()


def get_access_path_test():
    batting_table = CSVTable.CSVTable("batting")
    template = ["teamID", "playerID", "yearID"]
    index_result, count = batting_table.__get_access_path__(template)
    print(index_result)
    print(count)


# get_access_path_test()

def sub_where_template_test():
    # TODO: My changes here
    tab = CSVTable.CSVTable("people")
    template = {"nameFirst": "Hank", "nameSecond": "whatever", "nameLast": "Aaron"}


# sub_where_template_test()


def test_find_by_template_index():
    tab = CSVTable.CSVTable("people")
    template = {"nameFirst": "Hank", "nameLast": "Aaron"}
    result = tab.__find_by_template_index__(template, "name_index")
    print(result)


# test_find_by_template_index()

def smart_join_test():
    # TODO: My changes here
    pass


# smart_join_test()

# TODO: My test here
reset_db()
drop_tables_for_prep()
create_lahman_tables()
add_index_definitions()
test_load_info()
test_get_col_names()
add_other_indexes()
get_access_path_test()
test_find_by_template_index()

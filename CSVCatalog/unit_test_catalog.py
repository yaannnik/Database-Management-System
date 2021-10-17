import CSVCatalog
import json
import pymysql


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


# Example test, you will have to update the connection info
# Implementation Provided
def create_table_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="dbuserdbuser",
        db="CSVCatalog")
    cat.create_table("test_table", "./Appearances.csv")
    t = cat.get_table("test_table")
    print("Table = ", t)


# create_table_test()

def drop_table_test():
    # TODO: My changes here
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="dbuserdbuser",
        db="CSVCatalog")
    cat.drop_table("test_table")
    print("Drop test_table")


# drop_table_test()

def add_column_test():
    # TODO: My changes here
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="dbuserdbuser",
        db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("test_column", "text", False)
    t = cat.get_table("test_table")
    t.add_column_definition(col)


# add_column_test()

# Implementation Provided
# Fails because no name is given
def column_name_failure_test():
    cat = CSVCatalog.CSVCatalog()
    col = CSVCatalog.ColumnDefinition(None, "text", False)
    t = cat.get_table("test_table")
    t.add_column_definition(col)


# column_name_failure_test()

# Implementation Provided
# Fails because "canary" is not a permitted type
def column_type_failure_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="dbuserdbuser",
        db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("bird", "canary", False)
    t = cat.get_table("test_table")
    t.add_column_definition(col)


# column_type_failure_test()

# Implementation Provided
# Will fail because "happy" is not a boolean
def column_not_null_failure_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="dbuserdbuser",
        db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("name", "text", "happy")
    t = cat.get_table("test_table")
    t.add_column_definition(col)


# column_not_null_failure_test()


def add_index_test():
    # TODO: My changes here
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="dbuserdbuser",
        db="CSVCatalog")
    t = cat.get_table("test_table")
    t.define_index("test_index", ["test_column"], "INDEX")


# add_index_test()


def col_drop_test():
    # TODO: My changes here
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="dbuserdbuser",
        db="CSVCatalog")
    t = cat.get_table("test_table")
    t.drop_column_definition("test_column")


# col_drop_test()

def index_drop_test():
    # TODO: My changes here
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="dbuserdbuser",
        db="CSVCatalog")
    t = cat.get_table("test_table")
    t.drop_index("test_index")


# index_drop_test()

# Implementation provided
def describe_table_test():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("test_table")
    desc = t.describe_table()
    print("DESCRIBE test_table = \n", json.dumps(desc, indent=2))


# describe_table_test()

# TODO: My test here
def table_test():
    print("Reset database")
    reset_db()
    print("Test creating table")
    create_table_test()
    print("Describe test table")
    describe_table_test()
    print("Test dropping table")
    drop_table_test()


def column_and_index_test():
    print("Reset database")
    reset_db()
    print("Create  test table")
    create_table_test()

    # add column and  index
    print("Test Adding column")
    add_column_test()
    print("Test Adding index")
    add_index_test()
    print("Describe test table")
    describe_table_test()

    # drop column and index
    print("Test dropping index")
    index_drop_test()
    print("Test dropping column")
    col_drop_test()
    print("Describe test table")
    describe_table_test()
    print("Drop test table")
    drop_table_test()


def column_failure_tests():
    print("Reset database")
    reset_db()
    print("Create a test table")
    create_table_test()
    try:
        print("Test column name failure")
        column_name_failure_test()
    except ValueError as e:
        print("An error happened: ", e)

    try:
        print("Test column type failure")
        column_type_failure_test()
    except ValueError as e:
        print("An error happened: ", e)

    try:
        print("Test column not null failure")
        column_not_null_failure_test()
    except ValueError as e:
        print("An error happened: ", e)


# table_test()
# column_and_index_test()
# column_failure_tests()

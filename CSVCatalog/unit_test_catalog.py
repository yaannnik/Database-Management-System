import CSVCatalog
import json


# Example test, you will have to update the connection info
# Implementation Provided
def create_table_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="dbuserdbuser",
        db="CSVCatalog")
    cat.create_table("test_table", "file_path_test.woo")
    t = cat.get_table("test_table")
    print("Table = ", t)


# create_table_test()

def drop_table_test():
    # ************************ TO DO ***************************
    pass


# drop_table_test()

def add_column_test():
    # ************************ TO DO ***************************
    pass


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
        dbuser="dbuser",
        dbpw="dbuser",
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
        dbuser="dbuser",
        dbpw="dbuser",
        db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("name", "text", "happy")
    t = cat.get_table("test_table")
    t.add_column_definition(col)


# column_not_null_failure_test()


def add_index_test():
    # ************************ TO DO ***************************
    pass


# add_index_test()


def col_drop_test():
    # ************************ TO DO ***************************
    pass


# col_drop_test()

def index_drop_test():
    # ************************ TO DO ***************************
    pass


# index_drop_test()

# Implementation provided
def describe_table_test():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("test_table")
    desc = t.describe_table()
    print("DESCRIBE People = \n", json.dumps(desc, indent=2))

# describe_table_test()

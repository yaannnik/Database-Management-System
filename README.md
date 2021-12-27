# Database Management System on CSV Files

COMS-W4111@Columbia University. The whole project implemented a CSV based Database management systems. Source code and related test scripts are included.

## CSVTable

```txt
./CSVTable
├── data
│   ├── Baseball
│   │   ├── Appearances.csv
│   │   ├── Batting.csv
│   │   ├── People.csv
│   │   └── Pitching.csv
│   └── __init__.py
├── src
│   ├── BaseDataTable.py
│   ├── CSVDataTable.py
│   └── __init__.py
└── tests
    ├── __init__.py
    └── csv_table_tests.py
```

`CSVTable` contains the following files:

### data/

[Baseball/People.csv](./CSVTable/data/Baseball/People.csv)

[Baseball/Appearances.csv](./CSVTable/data/Baseball/Appearances.csv)

[Baseball/Batting.csv](./CSVTable/data/Baseball/Batting.csv)

[Baseball/Pitching.csv](./CSVTable/data/Baseball/Pitching.csv)

### src/

[BaseDataTable.py](./CSVTable/src/BaseDataTable.py): The base classes for CSV database, relational, etc. will extend this base class and implement the abstract methods.

[CSVDataTable.py](./CSVTable/src/CSVDataTable.py): The implementation classes for CSV database, relational extended from BaseDataTable.

### test/

[csv_table_tests.py](./CSVTable/tests/csv_table_tests.py): test files for CSVDataTable.

## CSVCatalog.

```txt
./CSVCatalog
├── HW3 README
├── data
│   ├── Appearances.csv
│   ├── Batting.csv
│   ├── People.csv
│   └── create.sql
├── src
│   ├── CSVCatalog.py
│   ├── CSVTable.py
│   ├── DataTableExceptions.py
│   └── __init__.py
└── test
    ├── __init__.py
    ├── unit_test_catalog.py
    ├── unit_test_catalog.txt
    ├── unit_test_csv_table.py
    └── unit_test_csv_table.txt
```

`CSVCatalog` contains the following files:

### data/

[People.csv](./CSVCatalog/data/People.csv)

[Appearances.csv](./CSVCatalog/data/Appearances.csv)

[Batting.csv](./CSVCatalog/data/Batting.csv)

[create.sql](./CSVCatalog/data/create.sql): SQL statements to create tables and constraints for the catalog.

### src/

[CSVCatalog.py](./CSVCatalog/src/CSVCatalog.py): Creates the methods that interact with the SQL database to store the metadata tables
This ensures table integrity is kept using MySQL rather than implementing integrity constraints ourselves. 

[CSVTable.py](./CSVCatalog/src/CSVTable.py): There are 4 classes created: ColumnDefinition, IndexDefinition, TableDefinition, and CSVCatalog.
CSV Catalog is the class the creates, drops, and loads a table.
A table definition is defined by file name and columns and indexes on those columns. These are created and column and index objects in ColumnDefiniton and IndexDefiniton.

[DataTableExceptions.py](./CSVCatalog/src/DataTableExceptions.py): A given file that raises specific exceptions.

Note: The create statements provided in [data/create.sql](./CSVCatalog/data/create.sql) must be run before executing CSVCatalog to ensure metadata tables are created correctly.

### test/

[unit_test_catalog.py](./CSVCatalog/test/unit_test_catalog.py): The test file provided and any additional tests your write.

[unit_test_csv_table.py](./CSVCatalog/test/unit_test_csv_table.py): A test file provided and any additional tests you write.

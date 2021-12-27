CREATE SCHEMA `CSVCatalog` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */;

USE CSVCatalog;

CREATE TABLE `csvtables` (
  `table_name` varchar(45) NOT NULL,
  `path` varchar(100) NOT NULL,
  PRIMARY KEY (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `csvcolumns` (
  `table_name` varchar(45) NOT NULL,
  `column_name` varchar(256) NOT NULL,
  `type` enum('text','number') NOT NULL,
  `not_null` tinyint(4) NOT NULL,
  PRIMARY KEY (`table_name`,`column_name`),
  CONSTRAINT `col_to_tbl` FOREIGN KEY (`table_name`) REFERENCES `csvtables` (`table_name`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `csvindexes` (
  `table_name` varchar(45) NOT NULL,
  `column_name` varchar(45) NOT NULL,
  `type` enum('PRIMARY','UNIQUE','INDEX') NOT NULL,
  `index_name` varchar(45) NOT NULL,
  `index_order` varchar(45) NOT NULL,
  PRIMARY KEY (`table_name`,`column_name`),
  CONSTRAINT `ind_to_col` FOREIGN KEY (`table_name`, `column_name`) REFERENCES `csvcolumns` (`table_name`, `column_name`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
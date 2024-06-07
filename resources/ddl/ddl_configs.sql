-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Server version:               8.0.19 - MySQL Community Server - GPL
-- Server OS:                    Win64
-- HeidiSQL Version:             12.6.0.6765
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- Dumping database structure for northwind
DROP DATABASE IF EXISTS `northwind`;
CREATE DATABASE IF NOT EXISTS `northwind` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `northwind`;

-- Dumping structure for table northwind.categories
DROP TABLE IF EXISTS `categories`;
CREATE TABLE IF NOT EXISTS `categories` (
  `CategoryID` int NOT NULL AUTO_INCREMENT,
  `CategoryName` varchar(15) NOT NULL,
  `Description` longtext,
  `Picture` longblob,
  PRIMARY KEY (`CategoryID`),
  UNIQUE KEY `CategoryName` (`CategoryName`)
) ENGINE=MyISAM AUTO_INCREMENT=9 DEFAULT CHARSET=utf8;

-- Data exporting was unselected.

-- Dumping structure for table northwind.customers
DROP TABLE IF EXISTS `customers`;
CREATE TABLE IF NOT EXISTS `customers` (
  `CustomerID` varchar(5) NOT NULL,
  `CompanyName` varchar(40) NOT NULL,
  `ContactName` varchar(30) DEFAULT NULL,
  `ContactTitle` varchar(30) DEFAULT NULL,
  `Address` varchar(60) DEFAULT NULL,
  `City` varchar(15) DEFAULT NULL,
  `Region` varchar(15) DEFAULT NULL,
  `PostalCode` varchar(10) DEFAULT NULL,
  `Country` varchar(15) DEFAULT NULL,
  `Phone` varchar(24) DEFAULT NULL,
  `Fax` varchar(24) DEFAULT NULL,
  `Image` longblob,
  `ImageThumbnail` longblob,
  PRIMARY KEY (`CustomerID`),
  KEY `City` (`City`),
  KEY `CompanyName` (`CompanyName`),
  KEY `PostalCode` (`PostalCode`),
  KEY `Region` (`Region`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- Data exporting was unselected.

-- Dumping structure for table northwind.employees
DROP TABLE IF EXISTS `employees`;
CREATE TABLE IF NOT EXISTS `employees` (
  `EmployeeID` int NOT NULL AUTO_INCREMENT,
  `LastName` varchar(20) NOT NULL,
  `FirstName` varchar(10) NOT NULL,
  `Title` varchar(30) DEFAULT NULL,
  `TitleOfCourtesy` varchar(25) DEFAULT NULL,
  `BirthDate` datetime DEFAULT NULL,
  `HireDate` datetime DEFAULT NULL,
  `Address` varchar(60) DEFAULT NULL,
  `City` varchar(15) DEFAULT NULL,
  `Region` varchar(15) DEFAULT NULL,
  `PostalCode` varchar(10) DEFAULT NULL,
  `Country` varchar(15) DEFAULT NULL,
  `HomePhone` varchar(24) DEFAULT NULL,
  `Extension` varchar(4) DEFAULT NULL,
  `Photo` longblob,
  `Notes` longtext,
  `ReportsTo` int DEFAULT NULL,
  PRIMARY KEY (`EmployeeID`),
  KEY `LastName` (`LastName`),
  KEY `PostalCode` (`PostalCode`)
) ENGINE=MyISAM AUTO_INCREMENT=10 DEFAULT CHARSET=utf8;

-- Data exporting was unselected.

-- Dumping structure for table northwind.orders
DROP TABLE IF EXISTS `orders`;
CREATE TABLE IF NOT EXISTS `orders` (
  `OrderID` int NOT NULL AUTO_INCREMENT,
  `CustomerID` varchar(5) DEFAULT NULL,
  `EmployeeID` int DEFAULT NULL,
  `OrderDate` datetime DEFAULT NULL,
  `RequiredDate` datetime DEFAULT NULL,
  `ShippedDate` datetime DEFAULT NULL,
  `ShipVia` int DEFAULT NULL,
  `Freight` decimal(19,4) DEFAULT '0.0000',
  `ShipName` varchar(40) DEFAULT NULL,
  `ShipAddress` varchar(60) DEFAULT NULL,
  `ShipCity` varchar(15) DEFAULT NULL,
  `ShipRegion` varchar(15) DEFAULT NULL,
  `ShipPostalCode` varchar(10) DEFAULT NULL,
  `ShipCountry` varchar(15) DEFAULT NULL,
  PRIMARY KEY (`OrderID`),
  KEY `CustomerID` (`CustomerID`),
  KEY `EmployeeID` (`EmployeeID`),
  KEY `OrderDate` (`OrderDate`),
  KEY `ShippedDate` (`ShippedDate`),
  KEY `ShipPostalCode` (`ShipPostalCode`)
) ENGINE=MyISAM AUTO_INCREMENT=11078 DEFAULT CHARSET=utf8;

-- Data exporting was unselected.

-- Dumping structure for table northwind.order_details
DROP TABLE IF EXISTS `order_details`;
CREATE TABLE IF NOT EXISTS `order_details` (
  `OrderID` int NOT NULL,
  `ProductID` int NOT NULL,
  `UnitPrice` decimal(19,4) NOT NULL DEFAULT '0.0000',
  `Quantity` int NOT NULL DEFAULT '1',
  `Discount` float NOT NULL DEFAULT '0',
  PRIMARY KEY (`OrderID`,`ProductID`),
  KEY `OrderID` (`OrderID`),
  KEY `ProductID` (`ProductID`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- Data exporting was unselected.

-- Dumping structure for table northwind.products
DROP TABLE IF EXISTS `products`;
CREATE TABLE IF NOT EXISTS `products` (
  `ProductID` int NOT NULL AUTO_INCREMENT,
  `ProductName` varchar(40) NOT NULL,
  `SupplierID` int DEFAULT NULL,
  `CategoryID` int DEFAULT NULL,
  `QuantityPerUnit` varchar(20) DEFAULT NULL,
  `UnitPrice` decimal(19,4) DEFAULT '0.0000',
  `UnitsInStock` int DEFAULT '0',
  `UnitsOnOrder` int DEFAULT '0',
  `ReorderLevel` int DEFAULT '0',
  `Discontinued` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`ProductID`),
  KEY `CategoryID` (`CategoryID`),
  KEY `ProductName` (`ProductName`),
  KEY `SupplierID` (`SupplierID`)
) ENGINE=MyISAM AUTO_INCREMENT=78 DEFAULT CHARSET=utf8;

-- Data exporting was unselected.

-- Dumping structure for table northwind.shippers
DROP TABLE IF EXISTS `shippers`;
CREATE TABLE IF NOT EXISTS `shippers` (
  `ShipperID` int NOT NULL AUTO_INCREMENT,
  `CompanyName` varchar(40) NOT NULL,
  `Phone` varchar(24) DEFAULT NULL,
  PRIMARY KEY (`ShipperID`)
) ENGINE=MyISAM AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;

-- Data exporting was unselected.

-- Dumping structure for table northwind.suppliers
DROP TABLE IF EXISTS `suppliers`;
CREATE TABLE IF NOT EXISTS `suppliers` (
  `SupplierID` int NOT NULL AUTO_INCREMENT,
  `CompanyName` varchar(40) NOT NULL,
  `ContactName` varchar(30) DEFAULT NULL,
  `ContactTitle` varchar(30) DEFAULT NULL,
  `Address` varchar(60) DEFAULT NULL,
  `City` varchar(15) DEFAULT NULL,
  `Region` varchar(15) DEFAULT NULL,
  `PostalCode` varchar(10) DEFAULT NULL,
  `Country` varchar(15) DEFAULT NULL,
  `Phone` varchar(24) DEFAULT NULL,
  `Fax` varchar(24) DEFAULT NULL,
  `HomePage` longtext,
  PRIMARY KEY (`SupplierID`),
  KEY `CompanyName` (`CompanyName`),
  KEY `PostalCode` (`PostalCode`)
) ENGINE=MyISAM AUTO_INCREMENT=30 DEFAULT CHARSET=utf8;

-- Data exporting was unselected.


-- Dumping database structure for stm_config
DROP DATABASE IF EXISTS `stm_config`;
CREATE DATABASE IF NOT EXISTS `stm_config` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `stm_config`;

-- Dumping structure for table stm_config.cf_cmp_compaction_config
DROP TABLE IF EXISTS `cf_cmp_compaction_config`;
CREATE TABLE IF NOT EXISTS `cf_cmp_compaction_config` (
  `appCode` varchar(400) DEFAULT NULL,
  `schemaName` varchar(400) DEFAULT NULL,
  `tableName` varchar(400) DEFAULT NULL,
  `isActive` varchar(400) DEFAULT NULL,
  `isHive` varchar(400) DEFAULT NULL,
  `rootPath` varchar(1000) DEFAULT NULL,
  `isFullCompactionRequired` varchar(400) DEFAULT NULL,
  `partitionFilter` varchar(1000) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table stm_config.cf_cmp_compaction_status
DROP TABLE IF EXISTS `cf_cmp_compaction_status`;
CREATE TABLE IF NOT EXISTS `cf_cmp_compaction_status` (
  `runId` varchar(400) DEFAULT NULL,
  `appCode` varchar(400) DEFAULT NULL,
  `schemaName` varchar(400) DEFAULT NULL,
  `tableName` varchar(400) DEFAULT NULL,
  `partionName` varchar(400) DEFAULT NULL,
  `fullPath` varchar(400) DEFAULT NULL,
  `fileCount` int DEFAULT NULL,
  `inputSize` varchar(400) DEFAULT NULL,
  `blockSize` varchar(400) DEFAULT NULL,
  `optimalPartitionCount` int DEFAULT NULL,
  `status` varchar(400) DEFAULT NULL,
  `errorMessage` varchar(400) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table stm_config.cf_cmp_dataset_config
DROP TABLE IF EXISTS `cf_cmp_dataset_config`;
CREATE TABLE IF NOT EXISTS `cf_cmp_dataset_config` (
  `stress_test_model` varchar(500) DEFAULT NULL,
  `ds_name` varchar(500) DEFAULT NULL,
  `source_of_df` varchar(500) DEFAULT NULL,
  `name_of_schema` varchar(500) DEFAULT NULL,
  `table_name` varchar(500) DEFAULT NULL,
  `src_path` varchar(500) DEFAULT NULL,
  `file_format` varchar(500) DEFAULT NULL,
  `delimiter` varchar(500) DEFAULT NULL,
  `partition_cols` varchar(500) DEFAULT NULL,
  `partition_filter_condition` varchar(500) DEFAULT NULL,
  `filter_condition` varchar(500) DEFAULT NULL,
  `filter_max_version_key` varchar(500) DEFAULT NULL,
  `select_cols` varchar(500) DEFAULT NULL,
  `is_query` varchar(500) DEFAULT NULL,
  `query` varchar(500) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table stm_config.cf_cmp_enrichment_rules
DROP TABLE IF EXISTS `cf_cmp_enrichment_rules`;
CREATE TABLE IF NOT EXISTS `cf_cmp_enrichment_rules` (
  `stress_test_model` varchar(500) DEFAULT NULL,
  `job_name` varchar(500) DEFAULT NULL,
  `process_name` varchar(500) DEFAULT NULL,
  `description` varchar(500) DEFAULT NULL,
  `priority` int DEFAULT NULL,
  `enrichment_rule` varchar(500) DEFAULT NULL,
  `enrichment_type` varchar(500) DEFAULT NULL,
  `target_col_name` varchar(500) DEFAULT NULL,
  `is_active` char(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table stm_config.cf_cmp_job_config
DROP TABLE IF EXISTS `cf_cmp_job_config`;
CREATE TABLE IF NOT EXISTS `cf_cmp_job_config` (
  `stress_test_model` varchar(500) DEFAULT NULL,
  `job_name` varchar(500) DEFAULT NULL,
  `ds_name` varchar(500) DEFAULT NULL,
  `ref_df` varchar(500) DEFAULT NULL,
  `target_type` varchar(500) DEFAULT NULL,
  `target_path` varchar(500) DEFAULT NULL,
  `write_mode` varchar(500) DEFAULT NULL,
  `file_format` varchar(500) DEFAULT NULL,
  `target_schema` varchar(500) DEFAULT NULL,
  `target_table_name` varchar(500) DEFAULT NULL,
  `target_partition_cols` varchar(500) DEFAULT NULL,
  `overwrite_condition` varchar(500) DEFAULT NULL,
  `target_primary_key_cols` varchar(500) DEFAULT NULL,
  `target_date_cols` varchar(500) DEFAULT NULL,
  `target_cols` varchar(500) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table stm_config.cf_cmp_join_config
DROP TABLE IF EXISTS `cf_cmp_join_config`;
CREATE TABLE IF NOT EXISTS `cf_cmp_join_config` (
  `stress_test_model` varchar(100) DEFAULT NULL,
  `job_name` varchar(100) DEFAULT NULL,
  `process_name` varchar(100) DEFAULT NULL,
  `priority` int DEFAULT NULL,
  `exec_sequence` int DEFAULT NULL,
  `join_description` varchar(100) DEFAULT NULL,
  `right_ds` varchar(100) DEFAULT NULL,
  `left_join_cols` varchar(1000) DEFAULT NULL,
  `right_join_cols` varchar(1000) DEFAULT NULL,
  `right_cols` varchar(1000) DEFAULT NULL,
  `join_prefix` varchar(100) DEFAULT NULL,
  `join_type` varchar(100) DEFAULT NULL,
  `is_broadcast_flag` varchar(100) DEFAULT NULL,
  `is_distinct_flag` varchar(100) DEFAULT NULL,
  `additional_join_config` varchar(1000) DEFAULT NULL,
  `left_table_ref` varchar(1000) DEFAULT NULL,
  `right_table_ref` varchar(1000) DEFAULT NULL,
  `join_query` varchar(1000) DEFAULT NULL,
  `exec_type` varchar(1000) DEFAULT NULL,
  `is_active` varchar(1000) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table stm_config.cf_cmp_result_table_config
DROP TABLE IF EXISTS `cf_cmp_result_table_config`;
CREATE TABLE IF NOT EXISTS `cf_cmp_result_table_config` (
  `stress_test_model` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `job_name` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `ds_name` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `target_type` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `target_path` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `write_mode` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `file_format` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `target_schema` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `target_table_name` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `target_partition_cols` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `overwrite_condition` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `target_primary_key_cols` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `target_date_cols` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `target_cols` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;

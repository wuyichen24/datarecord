CREATE DATABASE IF NOT EXISTS `test` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
USE `test`;
  
CREATE TABLE IF NOT EXISTS `GHSNV` (
  `RecordId` bigint(20) NOT NULL AUTO_INCREMENT,
  `SampleId` varchar(255) DEFAULT NULL,
  `RunId` varchar(255) DEFAULT NULL,
  `Gene` varchar(255) DEFAULT NULL,
  `Mutation_AA` varchar(255) DEFAULT NULL,
  `Percentage` double DEFAULT NULL,
  `Chrom` int(11) DEFAULT NULL,
  `Position` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`RecordId`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=latin1;

INSERT INTO GHSNV
(SampleId,RunId,Gene,Mutation_AA,Percentage,Chrom,Position) 
VALUES ('A09090101','160122_NB501062_0070_AHWNNNBGXX','EGFR', 'T790M', 10.5, 8, 1744567456);

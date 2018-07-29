# DataRecord

[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0) 

The transaction-style database connector for providing easy/high-consistency database operations.

## Overview

This database client is generic which supports to connect different types of relational databases, currently support MySQL, PostgreSQL and Oracle database. And this client also masks all the technical details for building connections to different types of databases, like the name of JDBC driver class, the pattern of JDBC URL. The only thing you need to provide just the database information, like host address, port number and database name, and the credential information, like username and password.

```java
GenericDbConfig config = new GenericDbConfig("localhost", "3306", "test", "root", "abcd1234");
DataRecordManager.buildConnection(DbType.MYSQL, config);      // build connection to MySQL
DataRecordManager.buildConnection(DbType.POSTGRESQL, config); // build connection to PostgreSQL
DataRecordManager.buildConnection(DbType.ORACLE, config);     // build connection to Oracle
```

This database client also Implements a commit pool which could cache the records in memory and you can decide the time to commit your change on the records. It is easy to roll back the change if there is an accident happened and improve database consistency.

```java
DataRecordManager.storeAndCommit();
```
This database client also provide you an easy way to do CRUD operations. For CRUD operation, you just need to modify the DataRecord which represents one record in the Database. When you synchronize DataRecord(s) with your database, it will automatically know each DataRecord is the new record which needs to be inserted, or the existing record which needs to be updated. And also it avoid you concatenating complex insert statements and update statements.

For DataRecord object itself, it is unlike the ResultSet, which will be closed automatically after the statement is closed, the DataRecord will persist the data in it after doing queries. Also unlike the ORM mechanism, you don’t have to create a Java class for each table respectively, it is lightweight and generic to do data manipulation.

## Getting Started
Please see our [Wiki](https://github.com/wuyichen24/datarecord/wiki/Getting-Started) page.

## Documentation
Please see our [Wiki](https://github.com/wuyichen24/datarecord/wiki) page.

## Download
- [Download ZIP](https://github.com/wuyichen24/datarecord/archive/master.zip)
- [Download JAR](https://github.com/wuyichen24/datarecord/releases/download/v1.1/boost-1.1.jar)

## Contributing

## License
[Apache-2.0](https://opensource.org/licenses/Apache-2.0)

## Authors
- **[Wuyi Chen](https://www.linkedin.com/in/wuyichen24/)**

# DataRecord

[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0) 

The transaction-style database connector for providing easy/high-consistency database operations.

## Overview

This database client is generic which supports to connect different types of relational databases, currently support MySQL, PostgreSQL and Oracle database. And this client also masks all the technical details for building connections to different types of databases, like the name of JDBC driver class, the pattern of JDBC URL. The only thing you need to provide just the database information, like host address, port number and database name, and the credential information, like username and password.

```java
GenericDbConfig config = new GenericDbConfig("localhost", "3306", "test", "root", "abcd1234");
DataRecordManager.buildConnection(DbType.MYSQL, config);
```

This database client also Implements a commit pool which could cache the records in memory and you can decide the time to commit your change on the records. It is easy to roll back the change if there is an accident happened and improve database consistency.

```java
DataRecordManager.storeAndCommit();
```

## Getting Started


## Documentation


## Download


## Contributing

## License
[Apache-2.0](https://opensource.org/licenses/Apache-2.0)

## Authors
- **[Wuyi Chen](https://www.linkedin.com/in/wuyichen24/)**

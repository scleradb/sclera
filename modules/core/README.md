# Sclera - Core
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.scleradb/sclera-core_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.scleradb/sclera-core_2.13)
[![scaladoc](https://javadoc.io/badge2/com.scleradb/sclera-core_2.13/scaladoc.svg)](https://javadoc.io/doc/com.scleradb/sclera-core_2.13)

This is the core Sclera SQL processor, which is responsible for parsing, optimizing and evaluating [SQL commands and queries](https://www.scleradb.com/docs/sclerasql/sqlintro/) with the help of the other components.

The SQL is processed in a streaming manner to the extent possible. The processor has the capability of reading data from any source (via connectors), and executiing arbitrary data transformations (as provided by plugins). This package also provides an extension API that can be used to develop such connectors and plugins.

For the details, please see the [technical details](https://www.scleradb.com/docs/intro/technical/) document.

This component includes an embedded [H2 database](http://www.h2database.com), which serves as the default [metadata store](https://www.scleradb.com/docs/intro/technical/#schema-store) and [data cache](https://www.scleradb.com/docs/intro/technical/#cache-store).

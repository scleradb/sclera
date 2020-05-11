# Sclera - JDBC Driver
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.scleradb/sclera-jdbc_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.scleradb/sclera-jdbc_2.13)
[![scaladoc](https://javadoc.io/badge2/com.scleradb/sclera-jdbc_2.13/scaladoc.svg)](https://javadoc.io/doc/com.scleradb/sclera-jdbc_2.13)

This component provides an embedded [JDBC](http://en.wikipedia.org/wiki/Java_Database_Connectivity) type 4 interface to Sclera.

The JDBC support is partial (for instance, functions related to transaction processing are not supported, and only forward scans of resultsets are permitted). However, the supported API should suffice for most analytics applications, and for interfacing with most JDBC-compliant BI tools.

A detailed description on how to use the JDBC API appears in the [Sclera JDBC Reference](https://www.scleradb.com/docs/interface/jdbc/) document.

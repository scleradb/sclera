/**
* Sclera - JDBC Driver
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.interfaces.jdbc

import com.scleradb.sql.parser.SqlParser

object Sql {
    val standardKeywords: List[String] = List(
        "ABSOLUTE", "ACTION", "ADD", "AFTER", "ALL", "ALLOCATE",
        "ALTER", "AND", "ANY", "ARE", "ARRAY", "AS", "ASC", "ASENSITIVE",
        "ASSERTION", "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "AVG",
        "BEFORE", "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BIT", "BIT_LENGTH",
        "BLOB", "BOOLEAN", "BOTH", "BREADTH", "BY", "CALL", "CALLED", "CASCADE",
        "CASCADED", "CASE", "CAST", "CATALOG", "CHAR", "CHARACTER",
        "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOB", "CLOSE", "COALESCE",
        "COLLATE", "COLLATION", "COLUMN", "COMMIT", "CONDITION", "CONNECT",
        "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONSTRUCTOR", "CONTAINS",
        "CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CROSS",
        "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP",
        "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE",
        "DATA", "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE",
        "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DEPTH", "DEREF", "DESC",
        "DESCRIBE", "DESCRIPTOR", "DETERMINISTIC", "DIAGNOSTICS", "DISCONNECT",
        "DISTINCT", "DO", "DOMAIN", "DOUBLE", "DROP", "DYNAMIC", "EACH",
        "ELEMENT", "ELSE", "ELSEIF", "END", "EQUALS", "ESCAPE", "EXCEPT",
        "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL", "EXTRACT",
        "FALSE", "FETCH", "FILTER", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND",
        "FREE", "FROM", "FULL", "FUNCTION", "GENERAL", "GET", "GLOBAL", "GO",
        "GOTO", "GRANT", "GROUP", "GROUPING", "HANDLER", "HAVING", "HOLD",
        "HOUR", "IDENTITY", "IF", "IMMEDIATE", "IN", "INDICATOR", "INITIALLY",
        "INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INT", "INTEGER",
        "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION", "ITERATE", "JOIN",
        "KEY", "LANGUAGE", "LARGE", "LAST", "LATERAL", "LEADING", "LEAVE",
        "LEFT", "LEVEL", "LIKE", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP",
        "LOCATOR", "LOOP", "LOWER", "MAP", "MATCH", "MAX", "MEMBER", "MERGE",
        "METHOD", "MIN", "MINUTE", "MODIFIES", "MODULE", "MONTH", "MULTISET",
        "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NEXT", "NO",
        "NONE", "NOT", "NULL", "NULLIF", "NUMERIC", "OBJECT", "OCTET_LENGTH",
        "OF", "OLD", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER",
        "ORDINALITY", "OUT", "OUTER", "OUTPUT", "OVER", "OVERLAPS", "PAD",
        "PARAMETER", "PARTIAL", "PARTITION", "PATH", "POSITION", "PRECISION",
        "PREPARE", "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE",
        "PUBLIC", "RANGE", "READ", "READS", "REAL", "RECURSIVE", "REF",
        "REFERENCES", "REFERENCING", "RELATIVE", "RELEASE", "REPEAT",
        "RESIGNAL", "RESTRICT", "RESULT", "RETURN", "RETURNS", "REVOKE",
        "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE", "ROW", "ROWS",
        "SAVEPOINT", "SCHEMA", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SECTION",
        "SELECT", "SENSITIVE", "SESSION", "SESSION_USER", "SET", "SETS",
        "SIGNAL", "SIMILAR", "SIZE", "SMALLINT", "SOME", "SPACE", "SPECIFIC",
        "SPECIFICTYPE", "SQL", "SQLCODE", "SQLERROR", "SQLEXCEPTION",
        "SQLSTATE", "SQLWARNING", "START", "STATE", "STATIC", "SUBMULTISET",
        "SUBSTRING", "SUM", "SYMMETRIC", "SYSTEM", "SYSTEM_USER", "TABLE",
        "TABLESAMPLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP",
        "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION",
        "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE", "UNDER",
        "UNDO", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UNTIL", "UPDATE",
        "UPPER", "USAGE", "USER", "USING", "VALUE", "VALUES", "VARCHAR",
        "VARYING", "VIEW", "WHEN", "WHENEVER", "WHERE", "WHILE", "WINDOW",
        "WITH", "WITHIN", "WITHOUT", "WORK", "WRITE", "YEAR", "ZONE"
    )

    def keywords(parser: SqlParser): List[String] =
        (parser.keywords -- standardKeywords).toList

    val numericFunctions: List[String] = List(
        "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEILING", "COS", "COT",
        "DEGREES", "EXP", "FLOOR", "LOG", "LOG10", "MOD", "PI", "POWER",
        "RADIANS", "ROUND", "SIGN", "SIN", "SQRT", "TAN", "TRUNCATE"
    )

    val stringFunctions: List[String] = List(
        "ASCII", "CHAR", "CONCAT", "INSERT", "LCASE", "LEFT", "LENGTH",
        "LOCATE", "LTRIM", "REPEAT", "REPLACE", "RIGHT", "RTRIM", "SPACE",
        "SUBSTRING", "UCASE"
    )

    val systemFunctions: List[String] = List("DATABASE", "IFNULL", "USER")

    val timeDateFunctions: List[String] = List(
        "CURDATE", "CURTIME", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK", "DAYOFYEAR",
        "HOUR", "MINUTE", "MONTH", "MONTHNAME", "NOW", "QUARTER", "SECOND",
        "WEEK", "YEAR"
    )
}

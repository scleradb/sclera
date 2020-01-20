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

import java.util.regex.Pattern

import java.sql.{RowIdLifetime, SQLDataException}

import java.sql.DatabaseMetaData.{columnNullable, columnNoNulls}
import java.sql.DatabaseMetaData.bestRowNotPseudo
import java.sql.DatabaseMetaData.{importedKeyNoAction, importedKeyNotDeferrable}
import java.sql.DatabaseMetaData.{typeNullable, typeSearchable}
import java.sql.DatabaseMetaData.sqlStateSQL

import scala.util.Random

import com.scleradb.sql.objects.{TableId, Table, SchemaTable, SchemaView}
import com.scleradb.sql.expr._
import com.scleradb.sql.types._
import com.scleradb.sql.statements._
import com.scleradb.sql.datatypes.{Column, PrimaryKey, ForeignKey}
import com.scleradb.sql.result.{TableResult, ScalTableResult}

import com.scleradb.dbms.location.{LocationId, Location}

class DatabaseMetaData(
    url: String,
    conn: Connection
) extends java.sql.DatabaseMetaData {
    override def getConnection(): java.sql.Connection = conn

    override def allProceduresAreCallable(): Boolean = true
    override def allTablesAreSelectable(): Boolean = true

    override def getURL(): String = url

    override def getUserName(): String = conn.getClientInfo("user")

    override def isReadOnly(): Boolean = conn.isReadOnly()

    override def nullsAreSortedHigh(): Boolean =
        SortAsc.defaultNullsOrder == NullsLast
    override def nullsAreSortedLow(): Boolean = !nullsAreSortedHigh()
    override def nullsAreSortedAtStart(): Boolean = false
    override def nullsAreSortedAtEnd(): Boolean = false

    override def getDatabaseProductName(): String =
        ScleraJdbcDriver.productName
    override def getDatabaseMajorVersion(): Int =
        ScleraJdbcDriver.productMajorVersion
    override def getDatabaseMinorVersion(): Int =
        ScleraJdbcDriver.productMinorVersion
    override def getDatabaseProductVersion(): String =
        s"${getDatabaseMajorVersion()}.${getDatabaseMinorVersion()}"

    override def getDriverName(): String = ScleraJdbcDriver.baseUrl
    override def getDriverMajorVersion(): Int = ScleraJdbcDriver.majorVersion
    override def getDriverMinorVersion(): Int = ScleraJdbcDriver.minorVersion
    override def getDriverVersion(): String =
        s"${getDriverMajorVersion()}.${getDriverMinorVersion()}"

    override def getJDBCMajorVersion(): Int = ScleraJdbcDriver.jdbcMajorVersion
    override def getJDBCMinorVersion(): Int = ScleraJdbcDriver.jdbcMinorVersion

    override def usesLocalFiles(): Boolean = false
    override def usesLocalFilePerTable(): Boolean = false

    override def supportsMixedCaseIdentifiers(): Boolean = false
    override def supportsMixedCaseQuotedIdentifiers(): Boolean = true

    override def storesMixedCaseIdentifiers(): Boolean = false
    override def storesUpperCaseIdentifiers(): Boolean = true
    override def storesLowerCaseIdentifiers(): Boolean = false
    override def storesUpperCaseQuotedIdentifiers(): Boolean = false
    override def storesLowerCaseQuotedIdentifiers(): Boolean = false
    override def storesMixedCaseQuotedIdentifiers(): Boolean = true

    override def getIdentifierQuoteString(): String = "\""

    override def getSQLKeywords(): String =
        Sql.keywords(conn.processor.parser).mkString(",")

    override def getNumericFunctions(): String =
        Sql.numericFunctions.mkString(",")

    override def getStringFunctions(): String =
        Sql.stringFunctions.mkString(",")

    override def getSystemFunctions(): String =
        Sql.systemFunctions.mkString(",")

    override def getTimeDateFunctions(): String =
        Sql.timeDateFunctions.mkString(",")

    override def getSearchStringEscape(): String = "\\"

    override def getExtraNameCharacters(): String = ""

    override def supportsAlterTableWithAddColumn(): Boolean = false
    override def supportsAlterTableWithDropColumn(): Boolean = false

    override def supportsColumnAliasing(): Boolean = true

    override def nullPlusNonNullIsNull(): Boolean = true

    override def supportsConvert(): Boolean = false
    override def supportsConvert(fromType: Int, toType: Int): Boolean = false

    override def supportsTableCorrelationNames(): Boolean = true
    override def supportsDifferentTableCorrelationNames(): Boolean = false

    override def supportsExpressionsInOrderBy(): Boolean = true
    override def supportsOrderByUnrelated(): Boolean = true

    override def supportsGroupBy(): Boolean = true
    override def supportsGroupByUnrelated(): Boolean = true
    override def supportsGroupByBeyondSelect(): Boolean = true

    override def supportsLikeEscapeClause(): Boolean = true

    override def supportsMultipleResultSets(): Boolean = false

    override def supportsMultipleTransactions(): Boolean = false

    override def supportsNonNullableColumns(): Boolean = true

    override def supportsMinimumSQLGrammar(): Boolean = true
    override def supportsCoreSQLGrammar(): Boolean = false
    override def supportsExtendedSQLGrammar(): Boolean = false

    override def supportsANSI92EntryLevelSQL(): Boolean = false
    override def supportsANSI92IntermediateSQL(): Boolean = false
    override def supportsANSI92FullSQL(): Boolean = false

    override def supportsIntegrityEnhancementFacility(): Boolean = false

    override def supportsOuterJoins(): Boolean = true
    override def supportsFullOuterJoins(): Boolean = true
    override def supportsLimitedOuterJoins(): Boolean = true

    override def getSchemaTerm(): String = "location"
    override def getProcedureTerm(): String = "function"
    override def getCatalogTerm(): String = "database"

    override def isCatalogAtStart(): Boolean = true
    override def getCatalogSeparator(): String = "."

    override def supportsSchemasInDataManipulation(): Boolean = true
    override def supportsSchemasInProcedureCalls(): Boolean = false
    override def supportsSchemasInTableDefinitions(): Boolean = true
    override def supportsSchemasInIndexDefinitions(): Boolean = false
    override def supportsSchemasInPrivilegeDefinitions(): Boolean = false

    override def supportsCatalogsInDataManipulation(): Boolean = false
    override def supportsCatalogsInProcedureCalls(): Boolean = false
    override def supportsCatalogsInTableDefinitions(): Boolean = false
    override def supportsCatalogsInIndexDefinitions(): Boolean = false
    override def supportsCatalogsInPrivilegeDefinitions(): Boolean = false

    override def supportsPositionedDelete(): Boolean = false
    override def supportsPositionedUpdate(): Boolean = false

    override def supportsSelectForUpdate(): Boolean = false

    override def supportsStoredProcedures(): Boolean = false

    override def supportsSubqueriesInComparisons(): Boolean = true
    override def supportsSubqueriesInExists(): Boolean = true
    override def supportsSubqueriesInIns(): Boolean = true
    override def supportsSubqueriesInQuantifieds(): Boolean = true
    override def supportsCorrelatedSubqueries(): Boolean = false

    override def supportsUnion(): Boolean = true
    override def supportsUnionAll(): Boolean = true

    override def supportsOpenCursorsAcrossCommit(): Boolean = false
    override def supportsOpenCursorsAcrossRollback(): Boolean = false

    override def supportsOpenStatementsAcrossCommit(): Boolean = false
    override def supportsOpenStatementsAcrossRollback(): Boolean = false

    override def getMaxBinaryLiteralLength(): Int = 767
    override def getMaxCharLiteralLength(): Int = 767
    override def getMaxColumnNameLength(): Int = 0 // not known

    override def getMaxColumnsInGroupBy(): Int = 0 // not known
    override def getMaxColumnsInIndex(): Int = 0 // not known
    override def getMaxColumnsInOrderBy(): Int = 0 // not known
    override def getMaxColumnsInSelect(): Int = 0 // not known
    override def getMaxColumnsInTable(): Int = 0 // not known

    override def getMaxConnections(): Int = 1 // no concurrency

    override def getMaxCursorNameLength(): Int = 0 // not known
    override def getMaxIndexLength(): Int = 0 // not known
    override def getMaxSchemaNameLength(): Int = 0 // not known
    override def getMaxProcedureNameLength(): Int = 0 // not known
    override def getMaxCatalogNameLength(): Int = 0 // not known
    override def getMaxTableNameLength(): Int = 0 // not known

    override def getMaxRowSize(): Int = 0 // not known
    override def doesMaxRowSizeIncludeBlobs(): Boolean = false
    override def getMaxStatementLength(): Int = 0 // not known
    override def getMaxStatements(): Int = 1 // no concurrency

    override def getMaxTablesInSelect(): Int = 0 // not known

    override def getMaxUserNameLength(): Int = 0 // not known

    override def getDefaultTransactionIsolation(): Int =
        java.sql.Connection.TRANSACTION_NONE
    override def supportsTransactions(): Boolean = false
    override def supportsTransactionIsolationLevel(level: Int): Boolean = false

    override def supportsDataDefinitionAndDataManipulationTransactions()
    : Boolean = false
    override def supportsDataManipulationTransactionsOnly(): Boolean = false
    override def dataDefinitionCausesTransactionCommit(): Boolean = false
    override def dataDefinitionIgnoredInTransactions(): Boolean = false

    override def getProcedures(
        catalog: String,
        schemaPattern: String,
        procedureNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("PROCEDURE_CAT", SqlCharVarying(None).option),
            Column("PROCEDURE_SCHEM", SqlCharVarying(None).option),
            Column("PROCEDURE_NAME", SqlCharVarying(None)),
            Column("REMARKS", SqlCharVarying(None)),
            Column("PROCEDURE_TYPE", SqlSmallInt),
            Column("SPECIFIC_NAME", SqlCharVarying(None))
        )

        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getProcedureColumns(
        catalog: String,
        schemaPattern: String,
        procedureNamePattern: String,
        columnNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("PROCEDURE_CAT", SqlCharVarying(None).option),
            Column("PROCEDURE_SCHEM", SqlCharVarying(None).option),
            Column("PROCEDURE_NAME", SqlCharVarying(None)),
            Column("COLUMN_NAME", SqlCharVarying(None)),
            Column("COLUMN_TYPE", SqlSmallInt),
            Column("DATA_TYPE", SqlSmallInt),
            Column("TYPE_NAME", SqlCharVarying(None)),
            Column("PRECISION", SqlInteger),
            Column("LENGTH", SqlInteger),
            Column("SCALE", SqlSmallInt),
            Column("RADIX", SqlSmallInt),
            Column("NULLABLE", SqlSmallInt),
            Column("REMARKS", SqlCharVarying(None)),
            Column("COLUMN_DEF", SqlCharVarying(None).option),
            Column("SQL_DATA_TYPE", SqlInteger),
            Column("SQL_DATETIME_SUB", SqlInteger),
            Column("CHAR_OCTECT_LENGTH", SqlInteger.option),
            Column("ORDINAL_POSITION", SqlInteger),
            Column("IS_NULLABLE", SqlCharVarying(None)),
            Column("SPECIFIC_NAME", SqlCharVarying(None))
        )

        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getTables(
        catalog: String,
        schemaPattern: String,
        tableNamePattern: String,
        types: Array[String]
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TABLE_CAT", SqlCharVarying(None).option),
            Column("TABLE_SCHEM", SqlCharVarying(None).option),
            Column("TABLE_NAME", SqlCharVarying(None)),
            Column("TABLE_TYPE", SqlCharVarying(None)),
            Column("REMARKS", SqlCharVarying(None)),
            Column("TYPE_CAT", SqlCharVarying(None).option),
            Column("TYPE_SCHEM", SqlCharVarying(None).option),
            Column("TYPE_NAME", SqlCharVarying(None).option),
            Column("SELF_REFERENCING_COL_NAME", SqlCharVarying(None).option),
            Column("REF_GENERATION", SqlCharVarying(None).option)
        )

        val tables: List[SchemaTable] =
            if( types contains "TABLE" )
                SchemaTable.objects(conn.processor.schema).filter { st =>
                    sqlPatternMatch(schemaPattern, st.locationId.repr) &&
                    sqlPatternMatch(tableNamePattern, st.obj.name)
                }
            else Nil

        val tVals: List[Map[String, ScalColValue]] = tables.map { st =>
            Map(
                "TABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                "TABLE_SCHEM" -> CharConst(st.locationId.repr),
                "TABLE_NAME" -> CharConst(st.obj.name),
                "TABLE_TYPE" -> CharConst("TABLE"),
                "REMARKS" -> CharConst(""),
                "TYPE_CAT" -> SqlNull(SqlCharVarying(None)),
                "TYPE_SCHEM" -> SqlNull(SqlCharVarying(None)),
                "TYPE_NAME" -> SqlNull(SqlCharVarying(None)),
                "SELF_REFERENCING_COL_NAME" -> SqlNull(SqlCharVarying(None)),
                "REF_GENERATION" -> SqlNull(SqlCharVarying(None))
            )
        }

        val views: List[SchemaView] =
            if( types contains "VIEW" )
                SchemaView.objects(conn.processor.schema).filter { sv =>
                    sqlPatternMatch(tableNamePattern, sv.obj.name)
                }
            else Nil

        val vVals: List[Map[String, ScalColValue]] = views.map { sv =>
            Map(
                "TABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                "TABLE_SCHEM" -> CharConst(""),
                "TABLE_NAME" -> CharConst(sv.obj.name),
                "TABLE_TYPE" -> CharConst("VIEW"),
                "REMARKS" -> CharConst(""),
                "TYPE_CAT" -> SqlNull(SqlCharVarying(None)),
                "TYPE_SCHEM" -> SqlNull(SqlCharVarying(None)),
                "TYPE_NAME" -> SqlNull(SqlCharVarying(None)),
                "SELF_REFERENCING_COL_NAME" -> SqlNull(SqlCharVarying(None)),
                "REF_GENERATION" -> SqlNull(SqlCharVarying(None))
            )
        }

        val sortedVals: List[Map[String, ScalColValue]] =
            (tVals:::vVals).sortBy { m =>
                (m("TABLE_TYPE"), m("TABLE_SCHEM"), m("TABLE_NAME")) match {
                    case (CharConst(a), CharConst(b), CharConst(c)) => (a, b, c)
                    case _ => ("", "", "")
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getSchemas(): java.sql.ResultSet = {
        val cols: List[Column] = List(
            Column("TABLE_SCHEM", SqlCharVarying(None)),
            Column("TABLE_CATALOG", SqlCharVarying(None).option)
        )

        val vals: List[Map[String, ScalColValue]] =
            Location.locationIds(conn.processor.schema).map { locId => 
                Map(
                    "TABLE_SCHEM" -> CharConst(locId.repr),
                    "TABLE_CATALOG" -> SqlNull(SqlCharVarying(None))
                )
            }

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                m("TABLE_SCHEM") match {
                    case CharConst(a) => a
                    case _ => ""
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getSchemas(
        catalog: String,
        schemaPattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TABLE_SCHEM", SqlCharVarying(None)),
            Column("TABLE_CATALOG", SqlCharVarying(None).option)
        )

        val locIds: List[LocationId] =
            Location.locationIds(conn.processor.schema).filter { locId =>
                sqlPatternMatch(schemaPattern, locId.repr)
            }

        val vals: List[Map[String, ScalColValue]] = locIds.map { locId => 
            Map(
                "TABLE_SCHEM" -> CharConst(locId.repr),
                "TABLE_CATALOG" -> SqlNull(SqlCharVarying(None))
            )
        }

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                m("TABLE_SCHEM") match {
                    case CharConst(a) => a
                    case _ => ""
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getCatalogs(): java.sql.ResultSet = {
        val cols: List[Column] = List(Column("TABLE_CAT", SqlCharVarying(None)))

        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getTableTypes(): java.sql.ResultSet = {
        val cols: List[Column] =
            List(Column("TABLE_TYPE", SqlCharVarying(None)))

        val vals: List[Map[String, ScalColValue]] = List(
            Map("TABLE_TYPE" -> CharConst("TABLE")),
            Map("TABLE_TYPE" -> CharConst("VIEW"))
        )

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                m("TABLE_TYPE") match {
                    case CharConst(a) => a
                    case _ => ""
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getColumns(
        catalog: String,
        schemaPattern: String,
        tableNamePattern: String,
        columnNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TABLE_CAT", SqlCharVarying(None).option),
            Column("TABLE_SCHEM", SqlCharVarying(None).option),
            Column("TABLE_NAME", SqlCharVarying(None)),
            Column("COLUMN_NAME", SqlCharVarying(None)),
            Column("DATA_TYPE", SqlInteger),
            Column("TYPE_NAME", SqlCharVarying(None)),
            Column("COLUMN_SIZE", SqlInteger),
            Column("BUFFER_LENGTH", SqlInteger.option),
            Column("DECIMAL_DIGITS", SqlInteger.option),
            Column("NUM_PREC_RADIX", SqlInteger),
            Column("NULLABLE", SqlInteger),
            Column("REMARKS", SqlCharVarying(None).option),
            Column("COLUMN_DEF", SqlCharVarying(None).option),
            Column("SQL_DATA_TYPE", SqlInteger.option),
            Column("SQL_DATETIME_SUB", SqlInteger.option),
            Column("CHAR_OCTET_LENGTH", SqlInteger.option),
            Column("ORDINAL_POSITION", SqlInteger),
            Column("IS_NULLABLE", SqlCharVarying(None)),
            Column("SCOPE_CATALOG", SqlCharVarying(None).option),
            Column("SCOPE_SCHEMA", SqlCharVarying(None).option),
            Column("SCOPE_TABLE", SqlCharVarying(None).option),
            Column("SOURCE_DATA_TYPE", SqlSmallInt.option),
            Column("IS_AUTOINCREMENT", SqlCharVarying(None)),
            Column("IS_GENERATEDCOLUMN", SqlCharVarying(None))
        )

        val tables: List[SchemaTable] =
            SchemaTable.objects(conn.processor.schema).filter { st =>
                sqlPatternMatch(schemaPattern, st.locationId.repr) &&
                sqlPatternMatch(tableNamePattern, st.obj.name)
            }

        val tableInfo: List[(Option[LocationId], String, List[Column])] =
            tables.map { t =>
                (Some(t.locationId), t.obj.name, t.obj.columns)
            }

        val views: List[SchemaView] =
            SchemaView.objects(conn.processor.schema).filter { sv =>
                sqlPatternMatch(tableNamePattern, sv.obj.name)
            }

        val viewInfo: List[(Option[LocationId], String, List[Column])] =
            views.map { v =>
                val emptyExpr: RelExpr =
                    RelOpExpr(Select(BoolConst(false)), List(v.obj.expr))
                val viewCols: List[Column] =
                    conn.processor.handleQueryStatement(
                        SqlRelQueryStatement(emptyExpr),
                        { (tableResult: TableResult) => tableResult.columns }
                    )

                (None, v.obj.name, viewCols)
            }
        
        val vals: List[Map[String, ScalColValue]] =
            (tableInfo:::viewInfo).flatMap { case (locIdOpt, tname, cols) =>
                cols.zipWithIndex.map { case (col, pos) =>
                    Map(
                        "TABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                        "TABLE_SCHEM" -> (
                            locIdOpt match {
                                case Some(locId) => CharConst(locId.repr)
                                case None => SqlNull(SqlCharVarying(None))
                            }
                        ),
                        "TABLE_NAME" -> CharConst(tname),
                        "COLUMN_NAME" -> CharConst(col.name),
                        "DATA_TYPE" -> IntConst(col.sqlType.sqlTypeCode),
                        "TYPE_NAME" -> CharConst(col.sqlType.repr),
                        "COLUMN_SIZE" -> (
                            SqlType.precisionOpt(col.sqlType) match {
                                case Some(n) => IntConst(n)
                                case None => SqlNull(SqlInteger)
                            }
                        ),
                        "BUFFER_LENGTH" -> SqlNull(SqlInteger),
                        "DECIMAL_DIGITS" -> (
                            SqlType.scaleOpt(col.sqlType) match {
                                case Some(n) => IntConst(n)
                                case None => SqlNull(SqlInteger)
                            }
                        ),
                        "NUM_PREC_RADIX" -> IntConst(10),
                        "NULLABLE" -> IntConst(
                            if( col.sqlType.isOption ) columnNullable
                            else columnNoNulls
                        ),
                        "REMARKS" -> SqlNull(SqlCharVarying(None)),
                        "COLUMN_DEF" -> SqlNull(SqlCharVarying(None)),
                        "SQL_DATA_TYPE" -> SqlNull(SqlInteger),
                        "SQL_DATETIME_SUB" -> SqlNull(SqlInteger),
                        "CHAR_OCTET_LENGTH" -> (
                            SqlType.charOctetLength(col.sqlType) match {
                                case Some(n) => IntConst(n)
                                case None => SqlNull(SqlInteger)
                            }
                        ),
                        "ORDINAL_POSITION" -> IntConst(pos+1),
                        "IS_NULLABLE" -> CharConst(
                            if( col.sqlType.isOption ) "YES" else "NO"
                        ),
                        "SCOPE_CATALOG" -> SqlNull(SqlCharVarying(None)),
                        "SCOPE_SCHEMA" -> SqlNull(SqlCharVarying(None)),
                        "SCOPE_TABLE" -> SqlNull(SqlCharVarying(None)),
                        "SOURCE_DATA_TYPE" -> SqlNull(SqlSmallInt),
                        "IS_AUTOINCREMENT" -> CharConst("NO"),
                        "IS_GENERATEDCOLUMN" -> CharConst("NO")
                    )
                }
            }

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                (m("TABLE_SCHEM"),
                 m("TABLE_NAME"), m("ORDINAL_POSITION")) match { 
                    case (CharConst(a), CharConst(b), IntConst(c)) => (a, b, c)
                    case _ => ("", "", -1)
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getColumnPrivileges(
        catalog: String,
        schema: String,
        table: String,
        columnNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TABLE_CAT", SqlCharVarying(None).option),
            Column("TABLE_SCHEM", SqlCharVarying(None).option),
            Column("TABLE_NAME", SqlCharVarying(None)),
            Column("COLUMN_NAME", SqlCharVarying(None)),
            Column("GRANTOR", SqlCharVarying(None)),
            Column("GRANTEE", SqlCharVarying(None)),
            Column("PRIVILEGE", SqlCharVarying(None)),
            Column("IS_GRANTABLE", SqlCharVarying(None))
        )
        
        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getTablePrivileges(
        catalog: String,
        schemaPattern: String,
        tableNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TABLE_CAT", SqlCharVarying(None).option),
            Column("TABLE_SCHEM", SqlCharVarying(None).option),
            Column("TABLE_NAME", SqlCharVarying(None)),
            Column("GRANTOR", SqlCharVarying(None)),
            Column("GRANTEE", SqlCharVarying(None)),
            Column("PRIVILEGE", SqlCharVarying(None)),
            Column("IS_GRANTABLE", SqlCharVarying(None))
        )
        
        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getBestRowIdentifier(
        catalog: String,
        schema: String,
        table: String,
        scope: Int,
        nullable: Boolean
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("SCOPE", SqlSmallInt),
            Column("COLUMN_NAME", SqlCharVarying(None)),
            Column("DATA_TYPE", SqlInteger),
            Column("TYPE_NAME", SqlCharVarying(None)),
            Column("COLUMN_SIZE", SqlInteger),
            Column("BUFFER_LENGTH", SqlInteger.option),
            Column("DECIMAL_DIGITS", SqlInteger.option),
            Column("PSEUDO_COLUMN", SqlSmallInt)
        )

        val locIdOpt: Option[LocationId] =
            if( schema == null ) None else Some(LocationId(schema))

        val relObjectRef: RelRefSource =
            RelExpr.relRefSource(conn.processor.schema, locIdOpt, table)

        val rCols: List[Column] = relObjectRef match {
            case (tableRef: TableRefSource) =>
                val tableObj: Table = try tableRef.table catch {
                    case (e: Throwable) =>
                        throw new java.sql.SQLDataException(e.getMessage, e)
                }

                tableObj.keyOpt match {
                    case Some(PrimaryKey(colRefs)) =>
                        colRefs.map { cref =>
                            tableObj.column(cref.name) getOrElse {
                                throw new java.sql.SQLDataException(
                                    new RuntimeException(
                                        "Primary key column \"" +
                                        cref.repr + "\" not found"
                                    )
                                )
                            }
                        }

                    case None => Nil
                }

            case (viewRef: ViewRef) => Nil
        }

        val vals: List[Map[String, ScalColValue]] = rCols.map { col =>
            Map(
                "SCOPE" -> ShortConst(scope.toShort),
                "COLUMN_NAME" -> CharConst(col.name),
                "DATA_TYPE" ->
                    IntConst(col.sqlType.sqlTypeCode),
                "TYPE_NAME" -> CharConst(col.sqlType.repr),
                "COLUMN_SIZE" -> (
                    SqlType.precisionOpt(col.sqlType) match {
                        case Some(n) => IntConst(n)
                        case None => SqlNull(SqlInteger)
                    }
                ),
                "BUFFER_LENGTH" -> SqlNull(SqlInteger),
                "DECIMAL_DIGITS" -> (
                    SqlType.scaleOpt(col.sqlType) match {
                        case Some(n) => IntConst(n)
                        case None => SqlNull(SqlInteger)
                    }
                ),
                "PSEUDO_COLUMN" -> ShortConst(bestRowNotPseudo)
            )
        }

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                (m("SCOPE"), m("COLUMN_NAME")) match { 
                    case (ShortConst(a), CharConst(b)) => (a, b)
                    case _ => (-1.toShort, "")
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getVersionColumns(
        catalog: String,
        schema: String,
        table: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("SCOPE", SqlSmallInt),
            Column("COLUMN_NAME", SqlCharVarying(None)),
            Column("DATA_TYPE", SqlInteger),
            Column("TYPE_NAME", SqlCharVarying(None)),
            Column("COLUMN_SIZE", SqlInteger),
            Column("BUFFER_LENGTH", SqlInteger.option),
            Column("DECIMAL_DIGITS", SqlInteger.option),
            Column("PSEUDO_COLUMN", SqlSmallInt)
        )

        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getPrimaryKeys(
        catalog: String,
        schema: String,
        table: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TABLE_CAT", SqlCharVarying(None).option),
            Column("TABLE_SCHEM", SqlCharVarying(None).option),
            Column("TABLE_NAME", SqlCharVarying(None)),
            Column("COLUMN_NAME", SqlCharVarying(None)),
            Column("KEY_SEQ", SqlSmallInt),
            Column("PK_NAME", SqlCharVarying(None).option)
        )

        val locIdOpt: Option[LocationId] =
            if( schema == null ) None else Some(LocationId(schema))

        val rObjectRef: RelRefSource =
            RelExpr.relRefSource(conn.processor.schema, locIdOpt, table)

        val vals: List[Map[String, ScalColValue]] = rObjectRef match {
            case (tableRef: TableRefSource) =>
                val tableObj: Table =
                    try tableRef.table catch {
                        case (e: Throwable) =>
                            throw new java.sql.SQLDataException(e.getMessage, e)
                    }

                val pkCols: List[ColRef] = tableObj.keyOpt match {
                    case Some(PrimaryKey(colRefs)) => colRefs
                    case None => Nil
                }

                pkCols.zipWithIndex.map { case (col, pos) =>
                    Map(
                        "TABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                        "TABLE_SCHEM" ->
                            CharConst(tableRef.locationId.repr),
                        "TABLE_NAME" -> CharConst(table),
                        "COLUMN_NAME" -> CharConst(col.name),
                        "KEY_SEQ" -> ShortConst((pos+1).toShort),
                        "PK_NAME" -> SqlNull(SqlCharVarying(None))
                    )
                }

            case (viewRef: ViewRef) => Nil
        }

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                m("COLUMN_NAME") match {
                    case CharConst(a) => a
                    case _ => ""
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getImportedKeys(
        catalog: String,
        schema: String,
        table: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("PKTABLE_CAT", SqlCharVarying(None).option),
            Column("PKTABLE_SCHEM", SqlCharVarying(None).option),
            Column("PKTABLE_NAME", SqlCharVarying(None)),
            Column("PKCOLUMN_NAME", SqlCharVarying(None)),
            Column("FKTABLE_CAT", SqlCharVarying(None).option),
            Column("FKTABLE_SCHEM", SqlCharVarying(None).option),
            Column("FKTABLE_NAME", SqlCharVarying(None)),
            Column("FKCOLUMN_NAME", SqlCharVarying(None)),
            Column("KEY_SEQ", SqlSmallInt),
            Column("UPDATE_RULE", SqlSmallInt),
            Column("DELETE_RULE", SqlSmallInt),
            Column("FK_NAME", SqlCharVarying(None).option),
            Column("PK_NAME", SqlCharVarying(None).option),
            Column("DEFERRABILITY", SqlSmallInt)
        )

        val fkRef: TableRefSource =
            if( schema == null )
                TableRefSourceByName(conn.processor.schema, table)
            else TableRefSourceById(
                    conn.processor.schema,
                    TableId(LocationId(schema), table)
                 )

        val fkTableObj: Table = try fkRef.table catch {
            case (e: Throwable) => 
                throw new java.sql.SQLDataException(e.getMessage, e)
        }

        val fks: List[ForeignKey] = fkTableObj.foreignKeys
        val vals: List[Map[String, ScalColValue]] =
            fks.flatMap { fk =>
                val pkRef: TableRefTarget =
                    fk.refTableTarget(conn.processor.schema)
                val pkTableObj: Table = try pkRef.table catch {
                    case (e: Throwable) =>
                        throw new java.sql.SQLDataException(e.getMessage, e)
                }

                val pk: PrimaryKey = pkTableObj.keyOpt getOrElse {
                    throw new java.sql.SQLDataException(
                        new RuntimeException(
                            "Foreign key in table " + table +
                            " references table " + pkTableObj.name +
                            " which does not have a primary key"
                        )
                    )
                }

                val fkCols: List[ColRef] = fk.cols
                val pkCols: List[ColRef] = pk.cols

                if( pkCols.size != fkCols.size )
                    throw new java.sql.SQLDataException(
                        new RuntimeException(
                            "Foreign key in table " + table +
                            " is not consistent with the primary key" +
                            " of referenced table " + pkTableObj.name
                        )
                    )

                pkCols.zip(fkCols).zipWithIndex.map {
                    case ((pcol, fcol), pos) =>
                        Map(
                            "PKTABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                            "PKTABLE_SCHEM" ->
                                CharConst(pkRef.tableId.locationId.repr),
                            "PKTABLE_NAME" -> CharConst(pkTableObj.name),
                            "PKCOLUMN_NAME" -> CharConst(pcol.name),
                            "FKTABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                            "FKTABLE_SCHEM" ->
                                CharConst(fkRef.tableId.locationId.repr),
                            "FKTABLE_NAME" -> CharConst(fkTableObj.name),
                            "FKCOLUMN_NAME" -> CharConst(fcol.name),
                            "KEY_SEQ" -> ShortConst((pos+1).toShort),
                            "UPDATE_RULE" -> ShortConst(importedKeyNoAction),
                            "DELETE_RULE" -> ShortConst(importedKeyNoAction),
                            "FK_NAME" -> SqlNull(SqlCharVarying(None)),
                            "PK_NAME" -> SqlNull(SqlCharVarying(None)),
                            "DEFERRABILITY" ->
                                ShortConst(importedKeyNotDeferrable)
                        )
                }
            }

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                (m("PKTABLE_SCHEM"), m("PKTABLE_NAME"), m("KEY_SEQ")) match {
                    case (CharConst(a), CharConst(b), ShortConst(c)) =>
                        (a, b, c)
                    case _ => ("", "", -1.toShort)
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getExportedKeys(
        catalog: String,
        schema: String,
        table: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("PKTABLE_CAT", SqlCharVarying(None).option),
            Column("PKTABLE_SCHEM", SqlCharVarying(None).option),
            Column("PKTABLE_NAME", SqlCharVarying(None)),
            Column("PKCOLUMN_NAME", SqlCharVarying(None)),
            Column("FKTABLE_CAT", SqlCharVarying(None).option),
            Column("FKTABLE_SCHEM", SqlCharVarying(None).option),
            Column("FKTABLE_NAME", SqlCharVarying(None)),
            Column("FKCOLUMN_NAME", SqlCharVarying(None)),
            Column("KEY_SEQ", SqlSmallInt),
            Column("UPDATE_RULE", SqlSmallInt),
            Column("DELETE_RULE", SqlSmallInt),
            Column("FK_NAME", SqlCharVarying(None).option),
            Column("PK_NAME", SqlCharVarying(None).option),
            Column("DEFERRABILITY", SqlSmallInt)
        )

        val pkRef: TableRefSource =
            if( schema == null )
                TableRefSourceByName(conn.processor.schema, table)
            else TableRefSourceById(
                    conn.processor.schema,
                    TableId(LocationId(schema), table)
                 )

        val pkTableObj: Table = try pkRef.table catch {
            case (e: Throwable) =>
                throw new java.sql.SQLDataException(e.getMessage, e)
        }

        val pk: PrimaryKey = pkTableObj.keyOpt getOrElse {
            throw new java.sql.SQLDataException(
                new RuntimeException(
                    "Requesting exported keys for table " + pkTableObj.name +
                    " which does not have a primary key"
                )
            )
        }

        val pkCols: List[ColRef] = pk.cols

        val vals: List[Map[String, ScalColValue]] =
            SchemaTable.objects(conn.processor.schema).flatMap { fkTable =>
                val fkTableObj: Table = fkTable.obj

                val fks: List[ForeignKey] =
                    fkTableObj.foreignKeys.filter { fk =>
                        fk.refTableId(conn.processor.schema).repr ==
                        pkRef.tableId.repr
                    }

                fks.flatMap { fk =>
                    val fkCols: List[ColRef] = fk.cols

                    if( pkCols.size != fkCols.size )
                        throw new java.sql.SQLDataException(
                            new RuntimeException(
                                "Foreign key in table " + fkTableObj.name +
                                " is not consistent with the primary key" +
                                " of referenced table " + pkTableObj.name
                            )
                        )

                    pkCols.zip(fkCols).zipWithIndex.map {
                        case ((pcol, fcol), pos) => Map(
                            "PKTABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                            "PKTABLE_SCHEM" ->
                                CharConst(pkRef.tableId.locationId.repr),
                            "PKTABLE_NAME" -> CharConst(pkTableObj.name),
                            "PKCOLUMN_NAME" -> CharConst(pcol.name),
                            "FKTABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                            "FKTABLE_SCHEM" ->
                                CharConst(fkTable.id.locationId.repr),
                            "FKTABLE_NAME" -> CharConst(fkTableObj.name),
                            "FKCOLUMN_NAME" -> CharConst(fcol.name),
                            "KEY_SEQ" -> ShortConst((pos+1).toShort),
                            "UPDATE_RULE" -> ShortConst(importedKeyNoAction),
                            "DELETE_RULE" -> ShortConst(importedKeyNoAction),
                            "FK_NAME" -> SqlNull(SqlCharVarying(None)),
                            "PK_NAME" -> SqlNull(SqlCharVarying(None)),
                            "DEFERRABILITY" ->
                                ShortConst(importedKeyNotDeferrable)
                        )
                    }
                }
            }

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                (m("FKTABLE_SCHEM"), m("FKTABLE_NAME"), m("KEY_SEQ")) match {
                    case (CharConst(a), CharConst(b), ShortConst(c)) =>
                        (a, b, c)
                    case _ => ("", "", -1.toShort)
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getCrossReference(
        parentCatalog: String,
        parentSchema: String,
        parentTable: String,
        foreignCatalog: String,
        foreignSchema: String,
        foreignTable: String
    ): java.sql.ResultSet = {
        if( parentCatalog != null ) throw new SQLDataException(
            "Catalog \"" + parentCatalog + "\" not found"
        )

        if( foreignCatalog != null ) throw new SQLDataException(
            "Catalog \"" + foreignCatalog + "\" not found"
        )

        val cols: List[Column] = List(
            Column("PKTABLE_CAT", SqlCharVarying(None).option),
            Column("PKTABLE_SCHEM", SqlCharVarying(None).option),
            Column("PKTABLE_NAME", SqlCharVarying(None)),
            Column("PKCOLUMN_NAME", SqlCharVarying(None)),
            Column("FKTABLE_CAT", SqlCharVarying(None).option),
            Column("FKTABLE_SCHEM", SqlCharVarying(None).option),
            Column("FKTABLE_NAME", SqlCharVarying(None)),
            Column("FKCOLUMN_NAME", SqlCharVarying(None)),
            Column("KEY_SEQ", SqlSmallInt),
            Column("UPDATE_RULE", SqlSmallInt),
            Column("DELETE_RULE", SqlSmallInt),
            Column("FK_NAME", SqlCharVarying(None).option),
            Column("PK_NAME", SqlCharVarying(None).option),
            Column("DEFERRABILITY", SqlSmallInt)
        )

        val pkRef: TableRefSource =
            if( parentSchema == null )
                TableRefSourceByName(conn.processor.schema, parentTable)
            else TableRefSourceById(
                    conn.processor.schema,
                    TableId(LocationId(parentSchema), parentTable)
                 )

        val pkTableObj: Table = try pkRef.table catch {
            case (e: Throwable) =>
                throw new java.sql.SQLDataException(e.getMessage, e)
        }

        val pk: PrimaryKey = pkTableObj.keyOpt getOrElse {
            throw new java.sql.SQLDataException(
                new RuntimeException(
                    "Requesting exported keys for table " + pkTableObj.name +
                    " which does not have a primary key"
                )
            )
        }

        val pkCols: List[ColRef] = pk.cols

        val fkRef: TableRefSource =
            if( foreignSchema == null )
                TableRefSourceByName(conn.processor.schema, foreignTable)
            else TableRefSourceById(
                    conn.processor.schema,
                    TableId(LocationId(foreignSchema), foreignTable)
                 )

        val fkTableObj: Table = try fkRef.table catch {
            case (e: Throwable) =>
                throw new java.sql.SQLDataException(e.getMessage, e)
        }

        val fks: List[ForeignKey] =
            fkTableObj.foreignKeys.filter { fk =>
                fk.refTableId(conn.processor.schema).repr == pkRef.tableId.repr
            }

        val vals: List[Map[String, ScalColValue]] =
            fks.flatMap { fk =>
                val fkCols: List[ColRef] = fk.cols

                if( pkCols.size != fkCols.size )
                    throw new java.sql.SQLDataException(
                        new RuntimeException(
                            "Foreign key in table " + fkTableObj.name +
                            " is not consistent with the primary key" +
                            " of referenced table " + pkTableObj.name
                        )
                    )

                pkCols.zip(fkCols).zipWithIndex.map {
                    case ((pcol, fcol), pos) => Map(
                        "PKTABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                        "PKTABLE_SCHEM" ->
                            CharConst(pkRef.tableId.locationId.repr),
                        "PKTABLE_NAME" -> CharConst(pkTableObj.name),
                        "PKCOLUMN_NAME" -> CharConst(pcol.name),
                        "FKTABLE_CAT" -> SqlNull(SqlCharVarying(None)),
                        "FKTABLE_SCHEM" ->
                            CharConst(fkRef.tableId.locationId.repr),
                        "PKTABLE_NAME" -> CharConst(fkTableObj.name),
                        "PKCOLUMN_NAME" -> CharConst(fcol.name),
                        "KEY_SEQ" -> ShortConst((pos+1).toShort),
                        "UPDATE_RULE" -> ShortConst(importedKeyNoAction),
                        "DELETE_RULE" -> ShortConst(importedKeyNoAction),
                        "FK_NAME" -> SqlNull(SqlCharVarying(None)),
                        "PK_NAME" -> SqlNull(SqlCharVarying(None)),
                        "DEFERRABILITY" ->
                            ShortConst(importedKeyNotDeferrable)
                    )
                }
            }

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                (m("FKTABLE_SCHEM"), m("FKTABLE_NAME"), m("KEY_SEQ")) match {
                    case (CharConst(a), CharConst(b), ShortConst(c)) =>
                        (a, b, c)
                    case _ => ("", "", -1.toShort)
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    def getTypeInfo(): java.sql.ResultSet = {
        val cols: List[Column] = List(
            Column("TYPE_NAME", SqlCharVarying(None)),
            Column("DATA_TYPE", SqlInteger),
            Column("PRECISION", SqlInteger.option),
            Column("LITERAL_PREFIX", SqlCharVarying(None).option),
            Column("LITERAL_SUFFIX", SqlCharVarying(None).option),
            Column("CREATE_PARAMS", SqlCharVarying(None).option),
            Column("NULLABLE", SqlSmallInt),
            Column("CASE_SENSITIVE", SqlBool),
            Column("SEARCHABLE", SqlSmallInt),
            Column("UNSIGNED_ATTRIBUTE", SqlBool),
            Column("FIXED_PREC_SCALE", SqlBool),
            Column("AUTO_INCREMENT", SqlBool),
            Column("LOCAL_TYPE_NAME", SqlCharVarying(None).option),
            Column("MINIMUM_SCALE", SqlSmallInt),
            Column("MAXIMUM_SCALE", SqlSmallInt),
            Column("SQL_DATA_TYPE", SqlInteger.option),
            Column("SQL_DATETIME_SUB", SqlInteger.option),
            Column("NUM_PREC_RADIX", SqlInteger)
        )

        val types: List[SqlType] = List(
            SqlBigInt, SqlBool, SqlCharFixed(None), SqlCharVarying(None),
            SqlDate, SqlDecimal(None, None), SqlFloat(None), SqlInteger,
            SqlNullType, SqlReal, SqlSmallInt, SqlText, SqlTime, SqlTimestamp
        )

        val vals: List[Map[String, ScalColValue]] = types.map { t =>
            Map(
                "TYPE_NAME" -> CharConst(t.repr),
                "DATA_TYPE" -> IntConst(t.sqlTypeCode),
                "PRECISION" -> (
                    SqlType.precisionOpt(t) match {
                        case Some(p) => IntConst(p)
                        case None => SqlNull(SqlCharVarying(None))
                    }
                ),
                "LITERAL_PREFIX" -> SqlNull(SqlCharVarying(None)),
                "LITERAL_SUFFIX" -> SqlNull(SqlCharVarying(None)),
                "CREATE_PARAMS" -> SqlNull(SqlCharVarying(None)),
                "NULLABLE" -> ShortConst(typeNullable),
                "CASE_SENSITIVE" -> (
                    t match {
                        case SqlCharFixed(_) | SqlCharVarying(_) | SqlText =>
                            BoolConst(true)
                        case _ => BoolConst(false)
                    }
                ),
                "SEARCHABLE" -> ShortConst(typeSearchable),
                "UNSIGNED_ATTRIBUTE" -> BoolConst(false),
                "FIXED_PREC_SCALE" -> (
                    t match {
                        case SqlInteger | SqlSmallInt | SqlBigInt |
                             SqlDecimal(_, _) | SqlFloat(_) | SqlReal =>
                            BoolConst(true)
                        case _ =>
                            BoolConst(false)
                    }
                ),
                "AUTO_INCREMENT" -> BoolConst(false),
                "LOCAL_TYPE_NAME" -> SqlNull(SqlCharVarying(None)),
                "MINIMUM_SCALE" -> (
                    SqlType.scaleOpt(t) match {
                        case None => ShortConst(0)
                        case Some(s) => ShortConst(s.toShort)
                    }
                ),
                "MAXIMUM_SCALE" -> (
                    SqlType.precisionOpt(t) match {
                        case None => SqlNull(SqlSmallInt)
                        case Some(p) => ShortConst(p.toShort)
                    }
                ),
                "SQL_DATA_TYPE" -> SqlNull(SqlInteger),
                "SQL_DATETIME_SUB" -> SqlNull(SqlInteger),
                "NUM_PREC_RADIX" -> IntConst(10)
            )
        }

        val sortedVals: List[Map[String, ScalColValue]] =
            vals.sortBy { m =>
                m("DATA_TYPE") match {
                    case IntConst(a) => a
                    case _ => -1
                }
            }

        new MetaDataResultSet(ScalTableResult(cols, sortedVals.iterator))
    }

    override def getIndexInfo(
        catalog: String,
        schema: String,
        table: String,
        unique: Boolean,
        approximate: Boolean
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TABLE_CAT", SqlCharVarying(None).option),
            Column("TABLE_SCHEM", SqlCharVarying(None).option),
            Column("TABLE_NAME", SqlCharVarying(None)),
            Column("NON_UNIQUE", SqlBool),
            Column("INDEX_QUALIFIER", SqlCharVarying(None).option),
            Column("INDEX_NAME", SqlCharVarying(None).option),
            Column("TYPE", SqlSmallInt),
            Column("ORDINAL_POSITION", SqlSmallInt),
            Column("COLUMN_NAME", SqlCharVarying(None).option),
            Column("ASC_OR_DESC", SqlCharVarying(None).option),
            Column("CARDINALITY", SqlInteger),
            Column("PAGES", SqlInteger),
            Column("FILTER_CONDITION", SqlCharVarying(None).option)
        )

        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def supportsResultSetType(t: Int): Boolean =
        (t == java.sql.ResultSet.TYPE_FORWARD_ONLY)

    override def supportsResultSetConcurrency(t: Int, concur: Int): Boolean = 
        supportsResultSetType(t) &&
        (concur == java.sql.ResultSet.CONCUR_READ_ONLY)

    override def ownUpdatesAreVisible(t: Int): Boolean = false
    override def ownDeletesAreVisible(t: Int): Boolean = false
    override def ownInsertsAreVisible(t: Int): Boolean = false

    override def othersUpdatesAreVisible(t: Int): Boolean = false
    override def othersDeletesAreVisible(t: Int): Boolean = false
    override def othersInsertsAreVisible(t: Int): Boolean = false

    override def updatesAreDetected(t: Int): Boolean = false
    override def deletesAreDetected(t: Int): Boolean = false
    override def insertsAreDetected(t: Int): Boolean = false

    override def supportsBatchUpdates(): Boolean = false

    override def getUDTs(
        catalog: String,
        schemaPattern: String,
        typeNamePattern: String,
        types: Array[Int]
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TYPE_CAT", SqlCharVarying(None).option),
            Column("TYPE_SCHEM", SqlCharVarying(None).option),
            Column("TYPE_NAME", SqlCharVarying(None)),
            Column("CLASS_NAME", SqlCharVarying(None)),
            Column("DATA_TYPE", SqlInteger),
            Column("REMARKS", SqlCharVarying(None)),
            Column("BASE_TYPE", SqlInteger.option)
        )
        
        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def supportsSavepoints(): Boolean = false
    override def supportsNamedParameters(): Boolean = false
    override def supportsMultipleOpenResults(): Boolean = false
    override def supportsGetGeneratedKeys(): Boolean = false

    override def getSuperTypes(
        catalog: String,
        schemaPattern: String,
        typeNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TYPE_CAT", SqlCharVarying(None).option),
            Column("TYPE_SCHEM", SqlCharVarying(None).option),
            Column("TYPE_NAME", SqlCharVarying(None)),
            Column("SUPERTYPE_CAT", SqlCharVarying(None).option),
            Column("SUPERTYPE_SCHEM", SqlCharVarying(None).option),
            Column("SUPERTYPE_NAME", SqlCharVarying(None))
        )
        
        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getSuperTables(
        catalog: String,
        schemaPattern: String,
        tableNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TABLE_CAT", SqlCharVarying(None).option),
            Column("TABLE_SCHEM", SqlCharVarying(None).option),
            Column("TABLE_NAME", SqlCharVarying(None)),
            Column("SUPERTABLE_NAME", SqlCharVarying(None))
        )
        
        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getAttributes(
        catalog: String,
        schemaPattern: String,
        typeNamePattern: String,
        attributeNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TYPE_CAT", SqlCharVarying(None).option),
            Column("TYPE_SCHEM", SqlCharVarying(None).option),
            Column("TYPE_NAME", SqlCharVarying(None)),
            Column("ATTR_NAME", SqlCharVarying(None)),
            Column("DATA_TYPE", SqlInteger),
            Column("ATTR_TYPE_NAME", SqlCharVarying(None)),
            Column("ATTR_SIZE", SqlInteger),
            Column("DECIMAL_DIGITS", SqlInteger.option),
            Column("NUM_PREC_RADIX", SqlInteger),
            Column("NULLABLE", SqlInteger),
            Column("REMARKS", SqlCharVarying(None).option),
            Column("ATTR_DEF", SqlCharVarying(None).option),
            Column("SQL_DATA_TYPE", SqlInteger.option),
            Column("SQL_DATETIME_SUB", SqlInteger.option),
            Column("CHAR_OCTET_LENGTH", SqlInteger.option),
            Column("ORDINAL_POSITION", SqlInteger),
            Column("IS_NULLABLE", SqlCharVarying(None)),
            Column("SCOPE_CATALOG", SqlCharVarying(None).option),
            Column("SCOPE_SCHEMA", SqlCharVarying(None).option),
            Column("SCOPE_TABLE", SqlCharVarying(None).option),
            Column("SOURCE_DATA_TYPE", SqlSmallInt.option)
        )
        
        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def supportsResultSetHoldability(
        holdability: Int
    ): Boolean = (holdability == getResultSetHoldability())

    override def getResultSetHoldability(): Int =
        java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT

    override def getSQLStateType(): Int = sqlStateSQL

    override def locatorsUpdateCopy(): Boolean = false

    override def supportsStatementPooling(): Boolean = false

    override def getRowIdLifetime(): RowIdLifetime =
        RowIdLifetime.ROWID_UNSUPPORTED

    override def supportsStoredFunctionsUsingCallSyntax(): Boolean = false

    override def autoCommitFailureClosesAllResultSets(): Boolean = false

    def getClientInfoProperties(): java.sql.ResultSet = {
        val cols: List[Column] = List(
            Column("NAME", SqlCharVarying(None)),
            Column("MAX_LEN", SqlInteger),
            Column("DEFAULT_VALUE", SqlCharVarying(None)),
            Column("DESCRIPTION", SqlCharVarying(None))
        )

        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getFunctions(
        catalog: String,
        schemaPattern: String,
        functionNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("FUNCTION_CAT", SqlCharVarying(None).option),
            Column("FUNCTION_SCHEM", SqlCharVarying(None).option),
            Column("FUNCTION_NAME", SqlCharVarying(None)),
            Column("REMARKS", SqlCharVarying(None)),
            Column("FUNCTION_TYPE", SqlSmallInt),
            Column("SPECIFIC_NAME", SqlCharVarying(None))
        )

        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getFunctionColumns(
        catalog: String,
        schemaPattern: String,
        functionNamePattern: String,
        columnNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("FUNCTION_CAT", SqlCharVarying(None).option),
            Column("FUNCTION_SCHEM", SqlCharVarying(None).option),
            Column("FUNCTION_NAME", SqlCharVarying(None)),
            Column("COLUMN_NAME", SqlCharVarying(None)),
            Column("COLUMN_TYPE", SqlSmallInt),
            Column("DATA_TYPE", SqlInteger),
            Column("TYPE_NAME", SqlCharVarying(None)),
            Column("PRECISION", SqlInteger),
            Column("LENGTH", SqlInteger),
            Column("SCALE", SqlSmallInt),
            Column("RADIX", SqlSmallInt),
            Column("NULLABLE", SqlSmallInt),
            Column("REMARKS", SqlCharVarying(None).option),
            Column("CHAR_OCTET_LENGTH", SqlInteger.option),
            Column("ORDINAL_POSITION", SqlInteger),
            Column("IS_NULLABLE", SqlCharVarying(None)),
            Column("SPECIFIC_NAME", SqlCharVarying(None))
        )

        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def getPseudoColumns(
        catalog: String,
        schemaPattern: String,
        tableNamePattern: String,
        columnNamePattern: String
    ): java.sql.ResultSet = {
        if( catalog != null )
            throw new SQLDataException("Catalog \"" + catalog + "\" not found")

        val cols: List[Column] = List(
            Column("TABLE_CAT", SqlCharVarying(None).option),
            Column("TABLE_SCHEM", SqlCharVarying(None).option),
            Column("TABLE_NAME", SqlCharVarying(None)),
            Column("COLUMN_NAME", SqlCharVarying(None)),
            Column("DATA_TYPE", SqlInteger),
            Column("COLUMN_SIZE", SqlInteger),
            Column("DECIMAL_DIGITS", SqlInteger.option),
            Column("NUM_PREC_RADIX", SqlInteger),
            Column("COLUMN_USAGE", SqlCharVarying(None)),
            Column("REMARKS", SqlCharVarying(None).option),
            Column("CHAR_OCTET_LENGTH", SqlInteger.option),
            Column("IS_NULLABLE", SqlCharVarying(None))
        )

        new MetaDataResultSet(ScalTableResult(cols))
    }

    override def generatedKeyAlwaysReturned(): Boolean = false

    // case insentitive SQL-compliant pattern match
    private def sqlPatternMatch(sqlPattern: String, s: String): Boolean =
        if( sqlPattern == null ) true else {
            val qP: String = "#P" + Random.nextInt(1000) + "P#"
            val qU: String = "#U" + Random.nextInt(1000) + "U#"

            val sqlPatternS: String =
                sqlPattern.replace(getSearchStringEscape() + "%", qP)
                .replace(getSearchStringEscape() + "_", qU)

            val regexS: String =
                escape(sqlPatternS).replace("%", ".*").replace("_", ".")

            val regex: String = regexS.replace(qP, "%").replace(qU, "_")

            Pattern.matches(regex.toUpperCase, s.toUpperCase)
        }

    private def escape(str: String): String = {
        val symbols: List[String] = List(
            "!", "^", "$", "&", "*", "+", "?", "|", ",", "-", ".",
            ":", "(", ")", "[", "]", "{", "}", "\\", "<", "=", ">"
        )

        symbols.foldLeft (str) {
            case (prev, s) => prev.replace(s, Pattern.quote(s))
        }
    }

    override def isWrapperFor(iface: java.lang.Class[_]) = false
    override def unwrap[T](iface: java.lang.Class[T]): T =
        throw new SQLDataException("Not a wrapper")
}

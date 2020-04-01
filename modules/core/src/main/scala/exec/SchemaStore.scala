/**
* Sclera - Core
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

package com.scleradb.exec

import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.io.{ObjectInputStream, ObjectOutputStream}

import java.sql.SQLException

import scala.language.postfixOps

import com.scleradb.util.encrypt.Encryptor
import com.scleradb.config.ScleraConfig

import com.scleradb.dbms.location._

import com.scleradb.objects._
import com.scleradb.api._

import com.scleradb.sql.types._
import com.scleradb.sql.objects.{TableId, ViewId}
import com.scleradb.sql.objects.{Table, SchemaTable, SchemaView}
import com.scleradb.sql.datatypes._
import com.scleradb.sql.expr._
import com.scleradb.sql.statements._
import com.scleradb.sql.parser._
import com.scleradb.sql.mapper.default.{ScleraSQLMapper => SqlMapper}

import com.scleradb.analytics.ml.objects.{MLObjectId, SchemaMLObject}

private[scleradb]
class SchemaStore(processor: Processor, schemaDbHandler: DbHandler) {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
    private val encryptor: Encryptor = Encryptor(ScleraConfig.encryptKey)

    private def inTransaction[T](f: => T): T =
        schemaDbHandler.inTransaction[T](f)

    def versionOpt: Option[String] = {
        val query: RelExpr = schemaTableRefSource("SENTINEL")
        val dbs: DbStruct[String] = StringStruct("VERSION")

        handleSchemaSingletonQuery(query, dbs)
    }

    def createSchemaTables() = inTransaction {
        schemaTables.foreach { st =>
            handleSchemaUpdate(SqlCreateDbObject(SqlTable(st.obj), Persistent))
        }

        handleSchemaUpdate(
            SqlInsert(
                schemaTableRefTarget("SENTINEL"),
                List(Row(List(CharConst(ScleraConfig.schemaVersion))))
            )
        )

        createByteStore()
    }

    def dropSchemaObjects() = inTransaction {
        schemaTables.reverse.foreach { st =>
            handleSchemaUpdate(SqlDropExplicit(st, Persistent))
        }

        dropByteStore()
    }

    def addLocation(loc: Location) = inTransaction {
        val locIdRepr: String = loc.id.repr

        val locRows: List[Row] =
            List(Row(List(CharConst(locIdRepr), CharConst(loc.permit.repr),
                          CharConst(loc.dbms), CharConst(loc.param),
                          loc.dbSchemaOpt.map(
                              s => CharConst(s)
                          ).getOrElse(SqlNull(SqlCharVarying(None))))))

        handleSchemaUpdate(
            SqlInsert(
                schemaTableRefTarget("LOCATIONS"),
                locRows
            )
        )

        val configRows: List[Row] = loc.config.map { case (key, value) =>
            val crypt: String = encryptor.encrypt(value)
            Row(List(CharConst(locIdRepr), CharConst(key), CharConst(crypt)))
        }

        if( !configRows.isEmpty )
            handleSchemaUpdate(
                SqlInsert(
                    schemaTableRefTarget("LOCATIONCONFIG"),
                    configRows
                )
            )
    }
        
    def addObject(
        obj: SchemaObject,
        deps: List[SchemaObjectId]
    ) = inTransaction {
        addObjectBase(obj.id)
        addDependencies(obj.id, deps)

        obj match {
            case (st: SchemaTable) => addTable(st)
            case (sv: SchemaView) => addView(sv)
            case (sm: SchemaMLObject) => addMLObject(sm)
            case _ =>
                throw new IllegalArgumentException(
                    "Cannot store object \"" + obj.id.repr + "\""
                )
        }
    }

    private def addObjectBase(id: SchemaObjectId): Unit = {
        val objectRows: List[Row] = List(Row(List(CharConst(id.repr))))
        handleSchemaUpdate(
            SqlInsert(
                schemaTableRefTarget("OBJECTS"),
                objectRows
            )
        )
    }

    def isObjectIdPresent(id: String): Boolean = {
        val query: RelExpr =
            schemaQuery("OBJECTS", "OBJECTID", id)
        val dbs: DbStruct[String] = StringStruct("OBJECTID")

        !handleSchemaSingletonQuery(query, dbs).isEmpty
    }

    private def addDependencies(
        objectId: SchemaObjectId,
        depObjectIds: List[SchemaObjectId]
    ): Unit = {
        val depRows: List[Row] = depObjectIds.zipWithIndex.map {
            case (depObjectId, depSeqNo) => Row(
                List(CharConst(objectId.repr),
                     IntConst(depSeqNo), CharConst(depObjectId.repr))
            )
        }

        if( !depRows.isEmpty )
            handleSchemaUpdate(
                SqlInsert(
                    schemaTableRefTarget("DEPENDENCIES"),
                    depRows
                )
            )
    }

    private def addTable(st: SchemaTable): Unit = {
        val tableRows: List[Row] =
            List(Row(List(CharConst(st.id.repr),
                          CharConst(st.obj.name),
                          CharConst(st.obj.baseType.repr),
                          CharConst(st.locationId.repr))))
        handleSchemaUpdate(
            SqlInsert(
                schemaTableRefTarget("TABLES"),
                tableRows
            )
        )

        val columnRows: List[Row] = st.obj.columns.zipWithIndex.map {
            case (Column(cname, sqlType, familyOpt), seqno) =>
                val tStr: String = SqlMapper.sqlTypeString(sqlType)
                val isNotNull: Boolean = sqlType match {
                    case SqlOption(_) => false
                    case _ => true
                }
                
                val familyStr: String = familyOpt.getOrElse(st.obj.name)

                Row(List(CharConst(st.id.repr), IntConst(seqno),
                         CharConst(cname), CharConst(tStr),
                         BoolConst(isNotNull), CharConst(familyStr)))
        }

        handleSchemaUpdate(
            SqlInsert(
                schemaTableRefTarget("COLUMNS"),
                columnRows
            )
        )

        st.obj.keyOpt.foreach {
            case PrimaryKey(colRefs) =>
                val keyColumnRows: List[Row] = colRefs.zipWithIndex.map {
                    case (ColRef(cname), seqno) =>
                        Row(List(CharConst(st.id.repr), IntConst(0),
                                 IntConst(seqno), CharConst(cname)))
                }

                handleSchemaUpdate(
                    SqlInsert(
                        schemaTableRefTarget("KEYCOLUMNS"),
                        keyColumnRows
                    )
                )
        }

        st.obj.foreignKeys.zipWithIndex.foreach {
            case (ForeignKey(fkCols, refLocOpt, refTableName, refCols), fkno) =>
                val keyId: Int = fkno + 1

                val fkColumnRows: List[Row] = fkCols.zipWithIndex.map {
                    case (ColRef(cname), seqno) =>
                        Row(List(CharConst(st.id.repr), IntConst(keyId),
                                 IntConst(seqno), CharConst(cname)))
                }

                handleSchemaUpdate(
                    SqlInsert(
                        schemaTableRefTarget("KEYCOLUMNS"),
                        fkColumnRows
                    )
                )

                if( !refCols.isEmpty ) {
                    val refColumnRows: List[Row] = refCols.zipWithIndex.map {
                        case (ColRef(cname), seqno) =>
                            Row(List(CharConst(st.id.repr), IntConst(keyId),
                                     IntConst(seqno), CharConst(cname)))
                    }

                    handleSchemaUpdate(
                        SqlInsert(
                            schemaTableRefTarget("REFKEYCOLUMNS"),
                            refColumnRows
                        )
                    )
                }

                val foreignKeyRows: List[Row] =
                    List(Row(List(CharConst(st.id.repr),
                                  IntConst(keyId),
                                  refLocOpt match {
                                      case Some(LocationId(refLoc)) =>
                                          CharConst(refLoc)
                                      case None =>
                                          SqlNull(SqlCharVarying(None))
                                  },
                                  CharConst(refTableName))))

                handleSchemaUpdate(
                    SqlInsert(
                        schemaTableRefTarget("FOREIGNKEYS"),
                        foreignKeyRows
                    )
                )
        }
    }

    private def addView(sv: SchemaView): Unit = {
        val id: String = sv.id.repr

        val bytesOut: ByteArrayOutputStream = new ByteArrayOutputStream()
        val objOut: ObjectOutputStream = new ObjectOutputStream(bytesOut)

        try {
            SchemaView.serialize(sv, objOut)
            objOut.flush()
            storeBytes(id, bytesOut.toByteArray())
        } finally objOut.close() // also closes bytesOut

        val viewRows: List[Row] = List(Row(List(CharConst(id))))
        handleSchemaUpdate(
            SqlInsert(
                schemaTableRefTarget("VIEWS"),
                viewRows
            )
        )
    }

    private def addMLObject(sm: SchemaMLObject): Unit = {
        val id: String = sm.id.repr

        val bytesOut: ByteArrayOutputStream = new ByteArrayOutputStream()
        val objOut: ObjectOutputStream = new ObjectOutputStream(bytesOut)

        try {
            SchemaMLObject.serialize(sm, objOut)
            objOut.flush()
            storeBytes(id, bytesOut.toByteArray())
        } finally objOut.close() // also closes bytesOut

        val mlObjectRows: List[Row] = List(Row(List(CharConst(id))))
        handleSchemaUpdate(
            SqlInsert(
                schemaTableRefTarget("MLOBJECTS"),
                mlObjectRows
            )
        )
    }

    def addIndexLocation(
        indexName: String,
        locationId: LocationId
    ) = inTransaction {
        val indexRows: List[Row] =
            List(Row(List(CharConst(indexName), CharConst(locationId.repr))))

        handleSchemaUpdate(
            SqlInsert(
                schemaTableRefTarget("INDEXLOCATIONS"),
                indexRows
            )
        )
    }

    def removeLocation(idStr: String) = inTransaction {
        val nameCols: List[(String, String)] = List(
            ("LOCATIONCONFIG", "LOCATIONID"),
            ("LOCATIONS", "LOCATIONID")
        )

        remove(nameCols, idStr)
    }

    def removeObject(id: SchemaObjectId) = inTransaction {
        val nameCols: List[(String, String)] = id match {
            case (_: TableId) => List(
                ("FOREIGNKEYS", "TABLEID"),
                ("KEYCOLUMNS", "TABLEID"),
                ("REFKEYCOLUMNS", "TABLEID"),
                ("COLUMNS", "TABLEID"),
                ("TABLES", "TABLEID"),
                ("DEPENDENCIES", "OBJECTID"),
                ("OBJECTS", "OBJECTID")
            )

            case (_: ViewId) => List(
                ("VIEWS", "VIEWID"),
                ("DEPENDENCIES", "OBJECTID"),
                ("OBJECTS", "OBJECTID")
            )

            case (_: MLObjectId) => List(
                ("MLOBJECTS", "MLOBJID"),
                ("DEPENDENCIES", "OBJECTID"),
                ("OBJECTS", "OBJECTID")
            )

            case _ =>
                throw new IllegalArgumentException(
                    "Cannot delete object \"" + id.repr + "\""
                )
        }

        val idStr: String = id.repr
        remove(nameCols, idStr)
        removeBytes(idStr)
    }

    private def remove(nameCols: List[(String, String)], cval: String): Unit = {
        nameCols.foreach {
            case (tname, cname) =>
                val tableId: TableId =
                    schemaTableRefTarget(tname).schemaTable.id
                val pred: ScalExpr = schemaEqPred(cname, cval)
                handleSchemaUpdate(SqlDelete(tableId, pred))
        }
    }

    def removeIndexLocation(indexName: String) = inTransaction {
        val nameCols: List[(String, String)] =
            List(("INDEXLOCATIONS", "INDEXNAME"))

        remove(nameCols, indexName)
    }

    def indexLocationOpt(indexName: String): Option[LocationId] = {
        val query: RelExpr =
            schemaQuery("INDEXLOCATIONS", "INDEXNAME", indexName)
        val dbs: DbStruct[String] = StringStruct("LOCATIONID")

        handleSchemaSingletonQuery(query, dbs).map {
            locName => LocationId(locName)
        }
    }

    def indexLocations: List[(String, LocationId)] = {
        val query: RelExpr = schemaTableRefSource("INDEXLOCATIONS")
        val dbs: DbStruct[(String, String)] =
            DbPairStruct(StringStruct("INDEXNAME"), StringStruct("LOCATIONID"))

        handleSchemaListQuery(query, dbs).map { case (indexName, locName) =>
            (indexName, LocationId(locName))
        }
    }

    def remLocationIds(idStrs: List[String]): List[LocationId] = {
        val query: RelExpr = schemaTableRefSource("LOCATIONS")
        val dbs: DbStruct[String] = StringStruct("LOCATIONID")

        val remIdStrs: List[String] =
            handleSchemaListQuery(query, dbs) diff idStrs

        remIdStrs.map { locid => LocationId(locid) }
    }

    def remLocations(idStrs: List[String]): List[Location] = {
        val query: RelExpr = schemaTableRefSource("LOCATIONS")
        val dbs:
        DbStruct[(String, (String, (String, (String, Option[String]))))] =
            DbPairStruct(
                StringStruct("LOCATIONID"), DbPairStruct(
                    StringStruct("PERMIT"), DbPairStruct(
                        StringStruct("DBMS"), DbPairStruct(
                             StringStruct("PARAM"),
                             DbOptionStruct(StringStruct("DBSCHEMA"))
                         )
                    )
                )
            )

        handleSchemaListQuery(query, dbs).filter {
            case (locid, _) => !idStrs.contains(locid)
        } map { case (locid, (permitStr, (dbms, (param, dbSchemaOpt)))) =>
            val configQuery: RelExpr =
                schemaQuery("LOCATIONCONFIG", "LOCATIONID", locid)
            val configDbs: DbStruct[(String, String)] = DbPairStruct(
                StringStruct("configKey"), StringStruct("CONFIGVALUE")
            )

            val config: List[(String, String)] =
                handleSchemaListQuery(configQuery, configDbs).map {
                    case (key, crypt) => (key, encryptor.decrypt(crypt))
                }

            Location(
                processor.schema, LocationId(locid), dbms, param,
                dbSchemaOpt, config, LocationPermit(permitStr)
            )
        }
    }

    def locationOpt(id: String): Option[Location] = {
        val query: RelExpr = schemaQuery("LOCATIONS", "LOCATIONID", id)
        val dbs: DbStruct[(String, (String, (String, Option[String])))] =
            DbPairStruct(
                StringStruct("PERMIT"), DbPairStruct(
                    StringStruct("DBMS"), DbPairStruct(
                         StringStruct("PARAM"),
                         DbOptionStruct(StringStruct("DBSCHEMA"))
                     )
                )
            )

        handleSchemaSingletonQuery(query, dbs).map {
            case (permitStr, (dbms, (param, dbSchemaOpt))) =>
                val configQuery: RelExpr =
                    schemaQuery("LOCATIONCONFIG", "LOCATIONID", id)
                val configDbs: DbStruct[(String, String)] = DbPairStruct(
                    StringStruct("configKey"), StringStruct("CONFIGVALUE")
                )

                val config: List[(String, String)] =
                    handleSchemaListQuery(configQuery, configDbs).map {
                        case (key, crypt) => (key, encryptor.decrypt(crypt))
                    }

                Location(
                    processor.schema, LocationId(id), dbms, param,
                    dbSchemaOpt, config, LocationPermit(permitStr)
                )
        }
    }

    def tables: List[SchemaTable] = {
        val query: RelExpr = schemaTableRefSource("TABLES")
        val dbs: DbStruct[(((String, String), String), String)] =
            DbPairStruct(
                DbPairStruct(
                    DbPairStruct(
                        StringStruct("TABLEID"),
                        StringStruct("TABLENAME")
                    ),
                    StringStruct("BASETYPE")
                ),
                StringStruct("LOCATIONID")
            )

        handleSchemaListQuery(query, dbs).map {
            case (((tableId, tname), baseTypeRepr), locIdStr) =>
                table(LocationId(locIdStr), tableId, tname, baseTypeRepr)
        }
    }

    def tables(locIds: List[LocationId]): List[SchemaTable] = {
        val query: RelExpr =
            schemaQuery("TABLES", "LOCATIONID", locIds.map { id => id.repr })
        val dbs: DbStruct[(((String, String), String), String)] =
            DbPairStruct(
                DbPairStruct(
                    DbPairStruct(
                        StringStruct("TABLEID"),
                        StringStruct("TABLENAME")
                    ),
                    StringStruct("BASETYPE")
                ),
                StringStruct("LOCATIONID")
            )

        val userTables: List[SchemaTable] =
            handleSchemaListQuery(query, dbs).map {
                case (((tableId, tname), baseTypeRepr), locIdStr) =>
                    table(LocationId(locIdStr), tableId, tname, baseTypeRepr)
            }

        if( locIds contains schemaDbHandler.location.id )
            schemaTables:::userTables
        else userTables
    }

    def tableOpt(tableId: String): Option[SchemaTable] = {
        val query: RelExpr = schemaQuery("TABLES", "TABLEID", tableId)
        val dbs: DbStruct[((String, String), String)] =
            DbPairStruct(
                DbPairStruct(
                    StringStruct("TABLENAME"),
                    StringStruct("BASETYPE")
                ),
                StringStruct("LOCATIONID")
            )

        handleSchemaSingletonQuery(query, dbs).map {
            case ((tname, baseTypeRepr), locIdStr) =>
                table(LocationId(locIdStr), tableId, tname, baseTypeRepr)
        } orElse schemaTableById.get(tableId)
    }

    private def table(
        locId: LocationId,
        tableId: String,
        tname: String,
        baseTypeRepr: String
    ): SchemaTable = {
        val columns: List[Column] = tableColumns(tableId, tname)

        val keyMap: Map[Int, List[ColRef]] = tableKeyMap("KEYCOLUMNS", tableId)
        val refKeyMap: Map[Int, List[ColRef]] =
            tableKeyMap("REFKEYCOLUMNS", tableId)

        val pkOpt: Option[PrimaryKey] =
            keyMap.get(0).map { cs => PrimaryKey(cs) }
        val fks: List[ForeignKey] =
            tableForeignKeys(tableId, keyMap, refKeyMap)

        val baseType: Table.BaseType = Table.BaseType(baseTypeRepr)

        SchemaTable(Table(tname, columns, pkOpt, fks, baseType), locId)
    }
        
    private def tableColumns(tableId: String, tname: String): List[Column] = {
        val cQuery: RelExpr =
            schemaQuery("COLUMNS", "TABLEID", tableId, "SEQNO")
        val cDbs: DbStruct[((String, String), (String, Boolean))] =
            DbPairStruct(
                DbPairStruct(
                    StringStruct("CNAME"),
                    StringStruct("FAMILY")
                ),
                DbPairStruct(
                    StringStruct("CTYPE"),
                    BoolStruct("ISNOTNULL")
                )
            )

        handleSchemaListQuery(cQuery, cDbs).map {
            case ((cname, familyStr), (cTypeStr, isNotNull)) =>
                val baseType: SqlType = processor.parser.parseSqlType(cTypeStr)
                val cType: SqlType =
                    if( isNotNull ) baseType else baseType.option
                val family: Option[String] =
                    if( familyStr.toUpperCase == tname.toUpperCase ) None
                    else Some(familyStr)
                Column(cname, cType, family)
        }
    }

    private def tableKeyMap(
        keySchemaTable: String,
        tableId: String
    ): Map[Int, List[ColRef]] = {
        val kQuery: RelExpr =
            schemaQuery(keySchemaTable, "TABLEID", tableId, "SEQNO")
        val kDbs: DbStruct[(Int, String)] =
            DbPairStruct(IntStruct("KEYID"), StringStruct("CNAME"))

        handleSchemaListQuery(kQuery, kDbs).groupBy(_._1).view.mapValues { kc =>
            kc.map { case (_, c) => ColRef(c) }
        } toMap
    }

    private def tableForeignKeys(
        tableId: String,
        keyMap: Map[Int, List[ColRef]],
        refKeyMap: Map[Int, List[ColRef]]
    ): List[ForeignKey] = {
        val fkQuery: RelExpr = schemaQuery("FOREIGNKEYS", "TABLEID", tableId)
        val fkDbs: DbStruct[(Int, (Option[String], String))] =
            DbPairStruct(
                IntStruct("KEYID"),
                DbPairStruct(
                    DbOptionStruct(StringStruct("REFTABLELOC")),
                    StringStruct("REFTABLENAME")
                )
            )

        handleSchemaListQuery(fkQuery, fkDbs).map {
            case (fk, (rLocOpt, rName)) =>
                ForeignKey(
                    keyMap(fk),
                    rLocOpt.map { rLoc => LocationId(rLoc) },
                    rName,
                    refKeyMap.get(fk) getOrElse Nil
                )
        }
    }

    def views: List[SchemaView] = {
        val query: RelExpr = schemaTableRefSource("VIEWS")
        val dbs: DbStruct[String] = StringStruct("VIEWID")

        handleSchemaListQuery(query, dbs).flatMap { viewId =>
            try Some(readViewDef(viewId)) catch { case (e: Throwable) =>
                logger.error(
                    "Cannot read view \"" + viewId + "\": " +
                    Option(e.getMessage).getOrElse("Reason unknown")
                )

                None
            }
        }
    }

    def viewOpt(viewId: String): Option[SchemaView] = {
        val query: RelExpr = schemaQuery("VIEWS", "VIEWID", viewId)
        val dbs: DbStruct[String] = StringStruct("VIEWID")

        handleSchemaSingletonQuery(query, dbs).map { _ => readViewDef(viewId) }
    }

    private def readViewDef(viewId: String): SchemaView = {
        val bytes: Array[Byte] = readBytes(viewId)

        val input: ByteArrayInputStream = new ByteArrayInputStream(bytes)
        val objIn: ObjectInputStream = new ObjectInputStream(input)

        try SchemaView.deSerialize(processor.schema, objIn)
        catch { case (e: Throwable) =>
            val msg: String =
                "Error reading view \"" + viewId + "\": " + e.getMessage()
            logger.warn(msg)
            throw e
        } finally {
            objIn.close() // also closes input
        }
    }

    def mlObjects: List[SchemaMLObject] = {
        val query: RelExpr = schemaTableRefSource("MLOBJECTS")
        val dbs: DbStruct[String] = StringStruct("MLOBJID")

        handleSchemaListQuery(query, dbs).map { mlObjId =>
            readMLObjectDef(mlObjId)
        }
    }

    def mlObjectOpt(mlObjId: String): Option[SchemaMLObject] = {
        val query: RelExpr = schemaQuery("MLOBJECTS", "MLOBJID", mlObjId)
        val dbs: DbStruct[String] = StringStruct("MLOBJID")

        handleSchemaSingletonQuery(query, dbs).map { _ =>
            readMLObjectDef(mlObjId)
        }
    }

    private def readMLObjectDef(mlObjId: String): SchemaMLObject = {
        val bytes: Array[Byte] = readBytes(mlObjId)

        val input: ByteArrayInputStream = new ByteArrayInputStream(bytes)
        val objIn: ObjectInputStream = new ObjectInputStream(input)

        try SchemaMLObject.deSerialize(objIn)
        catch { case (e: Throwable) =>
            val msg: String =
                "Error reading ML object \"" + mlObjId + "\": " + e.getMessage()
            logger.warn(msg)
            throw e
        } finally {
            objIn.close() // also closes input
        }
    }

    def dependencies(objectId: String): List[String] = {
        val sortExprs: List[SortExpr] = List(
            SortExpr(ColRef("DEPSEQNO"), SortAsc, SortAsc.defaultNullsOrder)
        )
        val sortOp: Order = Order(sortExprs)
        val baseQuery: RelExpr =
            schemaQuery("DEPENDENCIES", "OBJECTID", objectId)
        val query: RelExpr = RelOpExpr(sortOp, List(baseQuery))
        val dbs: DbStruct[String] = StringStruct("DEPOBJECTID")

        handleSchemaListQuery(query, dbs)
    }

    private def schemaEqPred(cname: String, cvals: List[String]): ScalExpr = {
        val eqs: List[ScalExpr] =
            cvals.map { cval => schemaEqPred(cname, cval) }

        if( eqs.isEmpty ) BoolConst(true) else {
            eqs.tail.foldLeft (eqs.head) {
                case (prev, nextEq) => ScalOpExpr(Or, List(prev, nextEq))
            }
        }
    }

    private def schemaEqPred(cname: String, cval: String): ScalExpr =
        ScalOpExpr(Equals, List(ColRef(cname), CharConst(cval)))

    private def schemaQuery(
        tname: String,
        cname: String,
        cvals: List[String]
    ): RelExpr =
        RelOpExpr(
            Select(schemaEqPred(cname, cvals)),
            List(schemaTableRefSource(tname))
        )

    private def schemaQuery(
        tname: String,
        cname: String,
        cval: String
    ): RelExpr = schemaQuery(tname, cname, List(cval))

    private def schemaQuery(
        tname: String,
        cname: String,
        cval: String,
        sortCol: String
    ): RelExpr = {
        val sortExpr: SortExpr =
            SortExpr(ColRef(sortCol), SortAsc, SortAsc.defaultNullsOrder)
        RelOpExpr(Order(List(sortExpr)), List(schemaQuery(tname, cname, cval)))
    }

    val maxLen: Int = 767
    val schemaTables: List[SchemaTable] = List(
        Table(
            "SENTINEL",
            List(Column("VERSION", SqlCharVarying(Some(maxLen)))),
            None,
            Nil,
            Table.BaseTable
        ),

        Table(
            "LOCATIONS",
            List(
                Column("LOCATIONID", SqlCharVarying(Some(maxLen))),
                Column("PERMIT", SqlCharVarying(Some(maxLen))),
                Column("DBMS", SqlCharVarying(Some(maxLen))),
                Column("PARAM", SqlCharVarying(Some(maxLen))),
                Column("DBSCHEMA", SqlCharVarying(Some(maxLen)).option)
            ),
            Some(PrimaryKey(List(ColRef("LOCATIONID")))),
            Nil,
            Table.BaseTable
        ),

        Table(
            "LOCATIONCONFIG",
            List(
                Column("LOCATIONID", SqlCharVarying(Some(maxLen))),
                Column("CONFIGKEY", SqlCharVarying(Some(maxLen))),
                Column("CONFIGVALUE", SqlCharVarying(Some(maxLen)))
            ),
            Some(PrimaryKey(List(ColRef("LOCATIONID"), ColRef("CONFIGKEY")))),
            List(
                ForeignKey(
                    List(ColRef("LOCATIONID")),
                    Some(schemaDbHandler.location.id),
                    "LOCATIONS"
                )
            ),
            Table.BaseTable
        ),

        Table(
            "OBJECTS",
            List(Column("OBJECTID", SqlCharVarying(Some(maxLen)))),
            Some(PrimaryKey(List(ColRef("OBJECTID")))),
            Nil,
            Table.BaseTable
        ),

        Table(
            "DEPENDENCIES",
            List(
                Column("OBJECTID", SqlCharVarying(Some(maxLen))),
                Column("DEPSEQNO", SqlInteger),
                Column("DEPOBJECTID", SqlCharVarying(Some(maxLen)))
            ),
            Some(PrimaryKey(List(ColRef("OBJECTID"), ColRef("DEPSEQNO")))),
            List(
                ForeignKey(
                    List(ColRef("OBJECTID")),
                    Some(schemaDbHandler.location.id),
                    "OBJECTS"
                ),
                ForeignKey(
                    List(ColRef("DEPOBJECTID")),
                    Some(schemaDbHandler.location.id),
                    "OBJECTS"
                )
            ),
            Table.BaseTable
        ),

        Table(
            "TABLES",
            List(
                Column("TABLEID", SqlCharVarying(Some(maxLen))),
                Column("TABLENAME", SqlCharVarying(Some(maxLen))),
                Column("BASETYPE", SqlCharVarying(Some(maxLen))),
                Column("LOCATIONID", SqlCharVarying(Some(maxLen)))
            ),
            Some(PrimaryKey(List(ColRef("TABLEID")))),
            List(
                ForeignKey(
                    List(ColRef("TABLEID")),
                    Some(schemaDbHandler.location.id), "OBJECTS"
                ),
                ForeignKey(
                    List(ColRef("LOCATIONID")),
                    Some(schemaDbHandler.location.id),
                    "LOCATIONS"
                )
            ),
            Table.BaseTable
        ),

        Table(
            "COLUMNS",
            List(
                Column("TABLEID", SqlCharVarying(Some(maxLen))),
                Column("SEQNO", SqlInteger),
                Column("CNAME", SqlCharVarying(Some(maxLen))),
                Column("CTYPE", SqlCharVarying(Some(maxLen))),
                Column("ISNOTNULL", SqlBool),
                Column("FAMILY", SqlCharVarying(Some(maxLen)))
            ),
            Some(PrimaryKey(List(ColRef("TABLEID"), ColRef("CNAME")))),
            List(
                ForeignKey(
                    List(ColRef("TABLEID")),
                    Some(schemaDbHandler.location.id),
                    "TABLES"
                )
            ),
            Table.BaseTable
        ),
        
        Table(
            "KEYCOLUMNS",
            List(
                Column("TABLEID", SqlCharVarying(Some(maxLen))),
                Column("KEYID", SqlInteger),
                Column("SEQNO", SqlInteger),
                Column("CNAME", SqlCharVarying(Some(maxLen)))
            ),
            Some(PrimaryKey(List(ColRef("TABLEID"),
                                 ColRef("KEYID"),
                                 ColRef("CNAME")))),
            List(
                ForeignKey(
                    List(ColRef("TABLEID"), ColRef("CNAME")),
                    Some(schemaDbHandler.location.id),
                    "COLUMNS"
                )
            ),
            Table.BaseTable
        ),

        Table(
            "REFKEYCOLUMNS",
            List(
                Column("TABLEID", SqlCharVarying(Some(maxLen))),
                Column("KEYID", SqlInteger),
                Column("SEQNO", SqlInteger),
                Column("CNAME", SqlCharVarying(Some(maxLen)))
            ),
            Some(PrimaryKey(List(ColRef("TABLEID"),
                                 ColRef("KEYID"),
                                 ColRef("CNAME")))),
            List(
                ForeignKey(
                    List(ColRef("TABLEID"), ColRef("CNAME")),
                    Some(schemaDbHandler.location.id),
                    "COLUMNS"
                )
            ),
            Table.BaseTable
        ),

        Table(
            "FOREIGNKEYS",
            List(
                Column("TABLEID", SqlCharVarying(Some(maxLen))),
                Column("KEYID", SqlInteger),
                Column("REFTABLELOC", SqlCharVarying(Some(maxLen)).option),
                Column("REFTABLENAME", SqlCharVarying(Some(maxLen)))
            ),
            Some(PrimaryKey(List(ColRef("TABLEID"), ColRef("KEYID")))),
            List(
                ForeignKey(
                    List(ColRef("TABLEID")),
                    Some(schemaDbHandler.location.id),
                    "TABLES"
                )
            ),
            Table.BaseTable
        ),
        
        Table(
            "INDEXLOCATIONS",
            List(
                Column("INDEXNAME", SqlCharVarying(Some(maxLen))),
                Column("LOCATIONID", SqlCharVarying(Some(maxLen)))
            ),
            Some(PrimaryKey(List(ColRef("INDEXNAME")))),
            List(
                ForeignKey(
                    List(ColRef("LOCATIONID")),
                    Some(schemaDbHandler.location.id),
                    "LOCATIONS"
                )
            ),
            Table.BaseTable
        ),

        Table(
            "VIEWS",
            List(Column("VIEWID", SqlCharVarying(Some(maxLen)))),
            Some(PrimaryKey(List(ColRef("VIEWID")))),
            List(
                ForeignKey(
                    List(ColRef("VIEWID")),
                    Some(schemaDbHandler.location.id),
                    "OBJECTS"
                )
            ),
            Table.BaseTable
        ),

        Table(
            "MLOBJECTS",
            List(Column("MLOBJID", SqlCharVarying(Some(maxLen)))),
            Some(PrimaryKey(List(ColRef("MLOBJID")))),
            List(
                ForeignKey(
                    List(ColRef("MLOBJID")),
                    Some(schemaDbHandler.location.id),
                    "OBJECTS"
                )
            ),
            Table.BaseTable
        )
    ).map { t => SchemaTable(t, schemaDbHandler.location.id) }

    private val schemaTableByName: Map[String, SchemaTable] = Map() ++
        schemaTables.map { st => st.obj.name -> st }

    private val schemaTableById: Map[String, SchemaTable] = Map() ++
        schemaTables.map { st => st.id.repr -> st }

    private def schemaTableRefSource(tname: String): TableRefSource =
        TableRefSourceExplicit(processor.schema, schemaTableByName(tname))

    private def schemaTableRefTarget(tname: String): TableRefTarget =
        TableRefTargetExplicit(processor.schema, schemaTableByName(tname))

    private def handleSchemaUpdate(stmt: SqlUpdateStatement): Unit = {
        val normalizedStmt: SqlUpdateStatement = 
            processor.normalizer.normalizeUpdateStatement(stmt)
        schemaDbHandler.handleUpdate(normalizedStmt)
    }

    private def handleSchemaListQuery[T](
        query: RelExpr,
        dbs: DbStruct[T]
    ): List[T] = processor.handleListQuery(query, dbs.fromDb)

    private def handleSchemaSingletonQuery[T](
        query: RelExpr,
        dbs: DbStruct[T]
    ): Option[T] = processor.handleSingletonQuery(query, dbs.fromDb)

    private def createByteStore(): Unit = schemaDbHandler.createByteStore()

    private def dropByteStore(): Unit = schemaDbHandler.dropByteStore()

    private def storeBytes(id: String, bytes: Array[Byte]): Unit =
        schemaDbHandler.storeBytes(id, bytes)

    private def readBytes(id: String): Array[Byte] =
        schemaDbHandler.readBytes(id)

    private def removeBytes(id: String): Unit =
        schemaDbHandler.removeBytes(id)
}

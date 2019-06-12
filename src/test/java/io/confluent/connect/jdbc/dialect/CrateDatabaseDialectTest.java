/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CrateDatabaseDialectTest extends BaseDialectTest<CrateDatabaseDialect> {

  @Override
  protected CrateDatabaseDialect createDialect() {
    return new CrateDatabaseDialect(sourceConfigWithUrl("jdbc:crate://something"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "SHORT");
    assertPrimitiveMapping(Type.INT16, "SHORT");
    assertPrimitiveMapping(Type.INT32, "INTEGER");
    assertPrimitiveMapping(Type.INT64, "LONG");
    assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "FLOAT");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.BYTES, "BYTE");
    assertPrimitiveMapping(Type.STRING, "STRING");
  }

  @Test
  public void shouldMapStructuredSchemaTypeToSqlTypes() {
    assertStructuredMapping(SchemaBuilder.struct().field("test", Schema.BOOLEAN_SCHEMA), Type.STRUCT, "OBJECT(STRICT) as (test BOOLEAN)");
    assertStructuredMapping(SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA), Type.MAP,"OBJECT(DYNAMIC)");
    assertStructuredMapping(SchemaBuilder.array(Schema.INT32_SCHEMA), Type.ARRAY, "ARRAY(INTEGER)");
  }

  private void assertStructuredMapping(SchemaBuilder schemaBuilder, Schema.Type expectedType, String expectedSqlType) {
    Schema schema = schemaBuilder.build();
    SinkRecordField field = new SinkRecordField(schema, schema.name(), false);
    String sqlType = dialect.getSqlType(field);
    assertEquals(expectedSqlType, sqlType);
  }

  @Test
  public void shouldMapDecimalSchemaTypeToFloatSqlType() {
    assertDecimalMapping(0, "FLOAT");
    assertDecimalMapping(3, "FLOAT");
    assertDecimalMapping(4, "FLOAT");
    assertDecimalMapping(5, "FLOAT");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("SHORT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SHORT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("LONG", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("STRING", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BYTE", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("FLOAT", Decimal.schema(0));
    verifyDataTypeMapping("TIMESTAMP", Date.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("TIMESTAMP");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("TIMESTAMP");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    SinkRecordField f9 = new SinkRecordField(
      SchemaBuilder.struct()
              .field("test", Schema.BOOLEAN_SCHEMA)
              .field("test2", Schema.STRING_SCHEMA)
              .field("test3", SchemaBuilder.struct().field("testA", Schema.INT32_SCHEMA).build())
            .build(),
      "c9",
      false);
    SinkRecordField f10 = new SinkRecordField(
            SchemaBuilder.array(Schema.STRING_SCHEMA),
            "c10",
            false);
    SinkRecordField f11 = new SinkRecordField(
            SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA),
            "c11",
            false);
    List<SinkRecordField> extendedSinkRecordFields = new ArrayList<>(sinkRecordFields);
    extendedSinkRecordFields.addAll(Arrays.asList(f9, f10, f11));

    assertEquals(
        "CREATE TABLE \"myTable\" (\n"
            + "\"c1\" INTEGER NOT NULL,\n"
            + "\"c2\" LONG NOT NULL,\n"
            + "\"c3\" STRING NOT NULL,\n"
            + "\"c4\" STRING,\n"
            + "\"c5\" TIMESTAMP,\n"
            + "\"c6\" TIMESTAMP,\n"
            + "\"c7\" TIMESTAMP,\n"
            + "\"c8\" FLOAT,\n"
            + "\"c9\" OBJECT(STRICT) as (test BOOLEAN, test2 STRING, test3 OBJECT(STRICT) as (testA INTEGER)) NOT NULL,\n"
            + "\"c10\" ARRAY(STRING) NOT NULL,\n"
            + "\"c11\" OBJECT(DYNAMIC) NOT NULL,\n"
            + "PRIMARY KEY(\"c1\"))",
        dialect.buildCreateTableStatement(tableId, extendedSinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "CREATE TABLE myTable (\n"
            + "c1 INTEGER NOT NULL,\n"
            + "c2 LONG NOT NULL,\n"
            + "c3 STRING NOT NULL,\n"
            + "c4 STRING,\n"
            + "c5 TIMESTAMP,\n"
            + "c6 TIMESTAMP,\n"
            + "c7 TIMESTAMP,\n"
            + "c8 FLOAT,\n"
            + "c9 OBJECT(STRICT) as (test BOOLEAN, test2 STRING, test3 OBJECT(STRICT) as (testA INTEGER)) NOT NULL,\n"
            + "c10 ARRAY(STRING) NOT NULL,\n"
            + "c11 OBJECT(DYNAMIC) NOT NULL,\n"
            + "PRIMARY KEY(c1))",
        dialect.buildCreateTableStatement(tableId, extendedSinkRecordFields)
    );
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    assertEquals(
        Arrays.asList(
            "ALTER TABLE \"myTable\" \n"
            + "ADD \"c1\" INTEGER NOT NULL,\n"
            + "ADD \"c2\" LONG NOT NULL,\n"
            + "ADD \"c3\" STRING NOT NULL,\n"
            + "ADD \"c4\" STRING,\n"
            + "ADD \"c5\" TIMESTAMP,\n"
            + "ADD \"c6\" TIMESTAMP,\n"
            + "ADD \"c7\" TIMESTAMP,\n"
            + "ADD \"c8\" FLOAT"
        ),
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        Arrays.asList(
            "ALTER TABLE myTable \n"
            + "ADD c1 INTEGER NOT NULL,\n"
            + "ADD c2 LONG NOT NULL,\n"
            + "ADD c3 STRING NOT NULL,\n"
            + "ADD c4 STRING,\n"
            + "ADD c5 TIMESTAMP,\n"
            + "ADD c6 TIMESTAMP,\n"
            + "ADD c7 TIMESTAMP,\n"
            + "ADD c8 FLOAT"
        ),
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildUpsertStatement() {
    assertEquals(
        "INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
        "\"columnC\",\"columnD\") VALUES (?,?,?,?,?,?) ON CONFLICT (\"id1\"," +
        "\"id2\") DO UPDATE SET \"columnA\"=EXCLUDED" +
        ".\"columnA\",\"columnB\"=EXCLUDED.\"columnB\",\"columnC\"=EXCLUDED" +
        ".\"columnC\",\"columnD\"=EXCLUDED.\"columnD\"",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "INSERT INTO myTable (id1,id2,columnA,columnB," +
        "columnC,columnD) VALUES (?,?,?,?,?,?) ON CONFLICT (id1," +
        "id2) DO UPDATE SET columnA=EXCLUDED" +
        ".columnA,columnB=EXCLUDED.columnB,columnC=EXCLUDED" +
        ".columnC,columnD=EXCLUDED.columnD",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
        System.lineSeparator() + "\"pk2\" INTEGER NOT NULL," + System.lineSeparator() +
        "\"col1\" INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    verifyCreateThreeColTwoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 INTEGER NOT NULL," +
        System.lineSeparator() + "pk2 INTEGER NOT NULL," + System.lineSeparator() +
        "col1 INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INTEGER");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" " + System.lineSeparator() + "ADD \"newcol1\" INTEGER," +
        System.lineSeparator() + "ADD \"newcol2\" INTEGER NOT NULL");
  }

  @Test
  public void upsert() {
    TableId customer = tableId("Customer");
    assertEquals(
        "INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") " +
         "VALUES (?,?,?,?) ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\"," +
         "\"salary\"=EXCLUDED.\"salary\",\"address\"=EXCLUDED.\"address\"",
        dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address")
        )
    );

    assertEquals(
            "INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") " +
                    "VALUES (?,?,?,?) ON CONFLICT (\"id\",\"name\",\"salary\",\"address\") DO NOTHING",
            dialect.buildUpsertQueryStatement(
                    customer,
                    columns(customer, "id", "name", "salary", "address"),
                    columns(customer)
            )
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "INSERT INTO Customer (id,name,salary,address) " +
        "VALUES (?,?,?,?) ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name," +
        "salary=EXCLUDED.salary,address=EXCLUDED.address",
        dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address")
        )
    );

    assertEquals(
            "INSERT INTO Customer (id,name,salary,address) " +
                    "VALUES (?,?,?,?) ON CONFLICT (id,name,salary,address) DO NOTHING",
            dialect.buildUpsertQueryStatement(
                    customer,
                    columns(customer, "id", "name", "salary", "address"),
                    columns(customer)
            )
    );
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:crate://localhost/test?user=fred&ssl=true",
        "jdbc:crate://localhost/test?user=fred&ssl=true"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:crate://localhost/test?user=fred&password=secret&ssl=true",
        "jdbc:crate://localhost/test?user=fred&password=****&ssl=true"
    );
  }

  @Test
  public void bindFieldStructSupported() throws SQLException {
    Schema substructSchema = SchemaBuilder.struct().field("sub_a", Schema.STRING_SCHEMA).build();
    Schema structSchema = SchemaBuilder.struct()
        .field("test", Schema.BOOLEAN_SCHEMA)
        .field("substruct", substructSchema)
        .build();
    Struct struct = new Struct(structSchema);
    struct.put("test", true);
    struct.put("substruct", new Struct(substructSchema).put("sub_a", "Hello, World!"));
    struct.validate();

    HashMap<String, Object> expectedMap = new HashMap<>();
    HashMap<String, Object> expectedSubMap = new HashMap<>();
    expectedSubMap.put("sub_a", "Hello, World!");
    expectedMap.put("test", true);
    expectedMap.put("substruct", expectedSubMap);

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    dialect.bindField(preparedStatement, 1, structSchema, struct);
    verify(preparedStatement, times(1)).setObject(1, expectedMap);
  }

  @Test
  public void bindFieldArraySupported() throws SQLException {
    Schema arraySchema = SchemaBuilder.array(Schema.INT8_SCHEMA);
    List<Integer> integers = new ArrayList<>(Arrays.asList(1, 1, 2, 3, 5, 8));

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    Array array = mock(Array.class);
    Connection connection = mock(Connection.class);
    Mockito.when(preparedStatement.getConnection()).thenReturn(connection);
    Mockito.when(connection.createArrayOf("short", integers.toArray())).thenReturn(array);

    dialect.bindField(preparedStatement, 1, arraySchema, integers);
    verify(connection, times(1)).createArrayOf("short", integers.toArray());
    verify(preparedStatement, times(1)).setArray(1, array);
  }

  @Test
  public void bindFieldMapSupported() throws SQLException {
    Schema mapSchema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA);

    HashMap<String, Object> expectedMap = new HashMap<>();
    expectedMap.put("field_a", 1);
    expectedMap.put("field_b", 9);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    dialect.bindField(preparedStatement, 1, mapSchema, expectedMap);
    verify(preparedStatement, times(1)).setObject(1, expectedMap);
  }

}



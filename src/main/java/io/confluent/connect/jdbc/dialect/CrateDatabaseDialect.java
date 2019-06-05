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

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;


/**
 * A {@link DatabaseDialect} for CrateDB.
 */
public class CrateDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link CrateDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(CrateDatabaseDialect.class.getSimpleName(), "cratedb");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new CrateDatabaseDialect(config);
    }
  }

  private static final String OBJECT_TYPE_NAME = "object";

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public CrateDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Override
  public String addFieldToSchema(ColumnDefinition columnDefn, SchemaBuilder builder) {
    final String fieldName = fieldNameFor(columnDefn);
    switch (columnDefn.type()) {
      case Types.BIT: {
        boolean optional = columnDefn.isOptional();
        int numBits = columnDefn.precision();
        Schema schema;
        if (numBits <= 1) {
          schema = optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
        } else if (numBits <= 8) {
          schema = optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
        } else {
          schema = optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
        }
        builder.field(fieldName, schema);
        return fieldName;
      }
      case Types.OTHER: {
        // Some of these types will have fixed size, but we drop this from the schema conversion
        // since only fixed byte arrays can have a fixed size
        if (isObjectType(columnDefn)) {
          builder.field(
              fieldName,
              columnDefn.isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA
          );
        }
        break;
      }
      default:
        break;
    }

    // Delegate for the remaining logic
    return super.addFieldToSchema(columnDefn, builder);
  }

  @Override
  public ColumnConverter createColumnConverter(
      ColumnMapping mapping
  ) {
    // First handle any PostgreSQL-specific types
    ColumnDefinition columnDefn = mapping.columnDefn();
    int col = mapping.columnNumber();
    switch (columnDefn.type()) {
      case Types.BIT: {
        // PostgreSQL allows variable length bit strings, but when length is 1 then the driver
        // returns a 't' or 'f' string value to represent the boolean value, so we need to handle
        // this as well as lengths larger than 8.
        final int numBits = columnDefn.precision();
        if (numBits <= 1) {
          return rs -> rs.getBoolean(col);
        } else if (numBits <= 8) {
          // Do this for consistency with earlier versions of the connector
          return rs -> rs.getByte(col);
        }
        return rs -> rs.getBytes(col);
      }
      case Types.OTHER: {
        if (isObjectType(columnDefn)) {
          return rs -> rs.getString(col);
        }
        break;
      }
      default:
        break;
    }

    // Delegate for the remaining logic
    return super.createColumnConverter(mapping);
  }

  protected boolean isObjectType(ColumnDefinition columnDefn) {
    String typeName = columnDefn.typeName();
    return OBJECT_TYPE_NAME.equalsIgnoreCase(typeName);
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    String type = getSchemaType(field.schema());
    if (type.isEmpty()) {
      return super.getSqlType(field);
    } else {
      return type;
    }
  }

  private String getSchemaType(Schema schema) {
    if (schema.name() != null) {
      switch (schema.name()) {
        case Decimal.LOGICAL_NAME:
          return "FLOAT";
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
      }
    }
    switch (schema.type()) {
      case INT8:
      case INT16:
        return "SHORT";
      case INT32:
        return "INTEGER";
      case INT64:
        return "LONG";
      case FLOAT32:
      case FLOAT64:
        return "FLOAT";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "STRING";
      case BYTES:
        return "BYTE";
      case STRUCT:
        StringBuilder definition = new StringBuilder("OBJECT(STRICT) as (");
        List<Field> fields = schema.fields();
        StringJoiner fieldsJoiner = new StringJoiner(", ");
        for (Field f : fields) {
          fieldsJoiner.add(f.name() + " " + getSchemaType(f.schema()));
        }
        definition.append(fieldsJoiner.toString());
        definition.append(")");
        return definition.toString();
      case MAP:
        return "OBJECT(DYNAMIC)";
      case ARRAY:
        return "ARRAY(" + getSchemaType(schema.valueSchema()) + ")";
      default:
        return "";
    }
  }

  @Override
  public void bindField(
          PreparedStatement statement,
          int index,
          Schema schema,
          Object value
  ) throws SQLException {
    if (value == null) {
      statement.setObject(index, null);
    } else {
      boolean bound = maybeBindLogical(statement, index, schema, value);
      if (!bound) {
        bound = maybeBindPrimitive(statement, index, schema, value);
      }
      if (!bound) {
        throw new ConnectException("Unsupported source data type: " + schema.type());
      }
    }
  }

  @Override
  protected boolean maybeBindPrimitive(
          PreparedStatement statement,
          int index,
          Schema schema,
          Object value
  ) throws SQLException {
    switch (schema.type()) {
      case INT8:
        statement.setByte(index, (Byte) value);
        break;
      case INT16:
        statement.setShort(index, (Short) value);
        break;
      case INT32:
        statement.setInt(index, (Integer) value);
        break;
      case INT64:
        statement.setLong(index, (Long) value);
        break;
      case FLOAT32:
        statement.setFloat(index, (Float) value);
        break;
      case FLOAT64:
        statement.setDouble(index, (Double) value);
        break;
      case BOOLEAN:
        statement.setBoolean(index, (Boolean) value);
        break;
      case STRING:
        statement.setString(index, (String) value);
        break;
      case STRUCT:
        statement.setObject(index, structToMap((Struct) value));
        break;
      case MAP:
        statement.setObject(index, value);
        break;
      case ARRAY:
        statement.setArray(index, statement.getConnection().createArrayOf(
            getSchemaType(schema.valueSchema()).toLowerCase(),
            ((ArrayList) value).toArray())
        );
        break;
      case BYTES:
        final byte[] bytes;
        if (value instanceof ByteBuffer) {
          final ByteBuffer buffer = ((ByteBuffer) value).slice();
          bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
        } else {
          bytes = (byte[]) value;
        }
        statement.setBytes(index, bytes);
        break;
      default:
        return false;
    }
    return true;
  }

  public Map<String, Object> structToMap(Struct struct) {
    Map<String, Object> map = new HashMap<>();
    for (Field field : struct.schema().fields()) {
      if (field.schema().type() == Schema.Type.STRUCT) {
        map.put(field.name(), structToMap((Struct) struct.get(field)));
      } else {
        map.put(field.name(), struct.get(field));
      }
    }
    return map;
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    final Transform<ColumnId> transform = (builder, col) -> {
      builder.appendColumnName(col.name())
             .append("=EXCLUDED.")
             .appendColumnName(col.name());
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(") ON CONFLICT (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns);
    if (nonKeyColumns.isEmpty()) {
      builder.append(") DO NOTHING");
    } else {
      builder.append(") DO UPDATE SET ");
      builder.appendList()
              .delimitedBy(",")
              .transformedBy(transform)
              .of(nonKeyColumns);
    }
    return builder.toString();
  }

  @Override
  public String buildCreateTableStatement(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();

    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
    if (!pkFieldNames.isEmpty()) {
      builder.append(",");
      builder.append(System.lineSeparator());
      builder.append("PRIMARY KEY(");
      builder.appendList()
          .delimitedBy(",")
          .transformedBy(ExpressionBuilder.quote())
          .of(pkFieldNames);
      builder.append(")");
    }
    builder.append(")");
    return builder.toString();
  }

  protected void writeColumnsSpec(
      ExpressionBuilder builder,
      Collection<SinkRecordField> fields
  ) {
    Transform<SinkRecordField> transform = (b, field) -> {
      b.append(System.lineSeparator());
      writeColumnSpec(b, field);
    };
    builder.appendList().delimitedBy(",").transformedBy(transform).of(fields);
  }

  protected void writeColumnSpec(
      ExpressionBuilder builder,
      SinkRecordField f
  ) {
    builder.appendColumnName(f.name());
    builder.append(" ");
    String sqlType = getSqlType(f);
    builder.append(sqlType);
    if (!isColumnOptional(f)) {
      builder.append(" NOT NULL");
    }
  }

}

package io.aiven.flink.connectors.bigquery.sink;

import static org.apache.flink.table.api.Expressions.row;

import java.time.Instant;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.provider.Arguments;

public class BigQuerySinkTest {
  static Stream<Arguments> datatypeProvider() {
    return Stream.of(
        Arguments.of(
            "decimal-test",
            new String[] {"decimal10_5_notNull"},
            new DataType[] {DataTypes.DECIMAL(10, 5).notNull()}),
        Arguments.of(
            "int-test", new String[] {"int_notNull"}, new DataType[] {DataTypes.INT().notNull()}),
        Arguments.of(
            "array-test",
            new String[] {"array_of_strings"},
            new DataType[] {DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull()}),
        Arguments.of(
            "array-row-array-string-int-test",
            new String[] {"row_of_string_int"},
            new DataType[] {
              DataTypes.ARRAY(
                      DataTypes.ROW(DataTypes.ARRAY(DataTypes.ROW(DataTypes.INT()).notNull()))
                          .notNull())
                  .notNull()
            }),
        Arguments.of(
            "row-array-row-array-string-int-test",
            new String[] {"row_of_string_int"},
            new DataType[] {
              DataTypes.ROW(
                      DataTypes.ARRAY(
                          DataTypes.ROW(DataTypes.ARRAY(DataTypes.INT().notNull())).notNull()),
                      DataTypes.INT())
                  .notNull()
            }),
        Arguments.of(
            "row-string-int-test",
            new String[] {"row_of_string_int"},
            new DataType[] {DataTypes.ROW(DataTypes.STRING(), DataTypes.INT()).notNull()}),
        Arguments.of(
            "row-row-string-int-test",
            new String[] {"row_row_of_string_int"},
            new DataType[] {
              DataTypes.ROW(
                      DataTypes.ROW(DataTypes.DECIMAL(4, 3)), DataTypes.STRING(), DataTypes.INT())
                  .notNull()
            }),
        Arguments.of(
            "row-row-row-string-int-test",
            new String[] {"row_row_row_of_string_int"},
            new DataType[] {
              DataTypes.ROW(
                      DataTypes.ROW(DataTypes.ROW(DataTypes.DATE()), DataTypes.DECIMAL(4, 3)),
                      DataTypes.STRING(),
                      DataTypes.INT())
                  .notNull()
            }),
        Arguments.of(
            "row-row-row-array-string-int-test",
            new String[] {"row_row_row_of_string_int"},
            new DataType[] {
              DataTypes.ROW(
                      DataTypes.ROW(
                          DataTypes.ROW(DataTypes.ARRAY(DataTypes.DATE().notNull())),
                          DataTypes.DECIMAL(4, 3)),
                      DataTypes.STRING(),
                      DataTypes.INT())
                  .notNull()
            }));
  }

  static Stream<Arguments> validTableDefinitionsProvider() {
    return Stream.of(
        Arguments.of(
            new String[] {"string", "string1"},
            new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
            new String[] {"string"},
            new DataType[] {DataTypes.STRING()},
            row("value")),
        Arguments.of(
            new String[] {"string", "string1"},
            new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
            new String[] {"string1"},
            new DataType[] {DataTypes.STRING()},
            row("value")),
        Arguments.of(
            new String[] {"string", "string1", "int", "date", "double"},
            new DataType[] {
              DataTypes.STRING(),
              DataTypes.STRING(),
              DataTypes.INT(),
              DataTypes.DATE(),
              DataTypes.DOUBLE()
            },
            new String[] {"int", "double"},
            new DataType[] {DataTypes.INT(), DataTypes.DOUBLE()},
            row(1, 3.14d)),
        Arguments.of(
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD("f1", DataTypes.STRING()), DataTypes.FIELD("f2", DataTypes.INT()))
            },
            new String[] {"row"},
            new DataType[] {DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING()))},
            row(Row.of("value1"))),
        Arguments.of(
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD("string", DataTypes.STRING()),
                  DataTypes.FIELD("int", DataTypes.INT()),
                  DataTypes.FIELD("date", DataTypes.DATE().notNull()),
                  DataTypes.FIELD("double", DataTypes.DOUBLE()),
                  DataTypes.FIELD("decimal", DataTypes.DECIMAL(3, 3)))
            },
            new String[] {"row"},
            new DataType[] {DataTypes.ROW(DataTypes.FIELD("date", DataTypes.DATE().notNull()))},
            row(Row.of(Instant.now()))),
        Arguments.of(
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD(
                      "row",
                      DataTypes.ROW(
                              DataTypes.FIELD("int", DataTypes.INT()),
                              DataTypes.FIELD("int2", DataTypes.INT().notNull()))
                          .notNull()),
                  DataTypes.FIELD("int", DataTypes.INT()),
                  DataTypes.FIELD("date", DataTypes.DATE().notNull()),
                  DataTypes.FIELD("double", DataTypes.DOUBLE()),
                  DataTypes.FIELD("decimal", DataTypes.DECIMAL(3, 3)))
            },
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD(
                      "row",
                      DataTypes.ROW(
                          DataTypes.FIELD("int", DataTypes.INT()),
                          DataTypes.FIELD("int2", DataTypes.INT().notNull()))),
                  DataTypes.FIELD("date", DataTypes.DATE().notNull()))
            },
            row(Row.of(Row.of(123, 234), Instant.now()))),
        Arguments.of(
            new String[] {"string", "string1", "int", "date", "double"},
            new DataType[] {
              DataTypes.STRING(),
              DataTypes.STRING(),
              DataTypes.INT(),
              DataTypes.DATE(),
              DataTypes.DOUBLE()
            },
            new String[] {"int", "double"},
            new DataType[] {DataTypes.INT(), DataTypes.DOUBLE()},
            row(1, 3.14d)),

        // Ideally this case should fail however because of Flink/Calcite issue it is not failed
        // during validation
        // instead it will fail during runtime if there is a NULL passed to not null column
        Arguments.of(
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD("f1", DataTypes.STRING()),
                  DataTypes.FIELD("f2", DataTypes.INT().notNull()))
            },
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD("f1", DataTypes.STRING()), DataTypes.FIELD("f2", DataTypes.INT()))
            },
            row(Row.of("value1", 11)),
            "Column #2 with name 'row.f2' is not nullable 'INT64' in BQ while in Flink it is nullable 'INT64'"));
  }

  static Stream<Arguments> invalidDefinitionsProvider() {
    return Stream.of(
        Arguments.of(
            new String[] {"invalid"},
            new DataType[] {DataTypes.STRING()},
            new String[] {"string"},
            new DataType[] {DataTypes.STRING()},
            row("value"),
            "There are unknown columns starting with #1 with name 'string'"),
        Arguments.of(
            new String[] {"string", "invalid"},
            new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
            new String[] {"string", "string2"},
            new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
            row("value1", "value2"),
            "There are unknown columns starting with #2 with name 'string2'"),
        Arguments.of(
            new String[] {"field"},
            new DataType[] {DataTypes.INT()},
            new String[] {"field"},
            new DataType[] {DataTypes.STRING()},
            row("value"),
            "Column #1 with name 'field' has type 'INT64' in BQ while in Flink it has type 'STRING'"),
        Arguments.of(
            new String[] {"field1", "field2"},
            new DataType[] {DataTypes.STRING(), DataTypes.INT()},
            new String[] {"field1", "field2"},
            new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
            row("value1", "value2"),
            "Column #2 with name 'field2' has type 'INT64' in BQ while in Flink it has type 'STRING'"),
        Arguments.of(
            new String[] {"string"},
            new DataType[] {DataTypes.STRING().notNull()},
            new String[] {"string"},
            new DataType[] {DataTypes.STRING()},
            row("value"),
            "Column #1 with name 'string' is not nullable 'STRING' in BQ while in Flink it is nullable 'STRING'"),
        Arguments.of(
            new String[] {"string1", "string2"},
            new DataType[] {DataTypes.STRING().notNull(), DataTypes.STRING().notNull()},
            new String[] {"string1", "string2"},
            new DataType[] {DataTypes.STRING().notNull(), DataTypes.STRING()},
            row("value1", "value2"),
            "Column #2 with name 'string2' is not nullable 'STRING' in BQ while in Flink it is nullable 'STRING'"),
        Arguments.of(
            new String[] {"array"},
            new DataType[] {DataTypes.ARRAY(DataTypes.STRING().notNull())},
            new String[] {"array"},
            new DataType[] {DataTypes.ARRAY(DataTypes.STRING().nullable()).nullable()},
            row(new String[] {"value1"}),
            "Type ARRAY<STRING> is not supported (nullable elements of array are not supported by BQ)"),
        Arguments.of(
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD("f1", DataTypes.STRING()), DataTypes.FIELD("f2", DataTypes.INT()))
            },
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD("invalid", DataTypes.STRING()),
                  DataTypes.FIELD("f2", DataTypes.INT()))
            },
            row(Row.of("value1", 11)),
            "There are unknown columns starting with #1 with name 'row.invalid'"),
        Arguments.of(
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD("f1", DataTypes.STRING()), DataTypes.FIELD("f2", DataTypes.INT()))
            },
            new String[] {"row"},
            new DataType[] {
              DataTypes.ROW(
                  DataTypes.FIELD("f1", DataTypes.STRING()),
                  DataTypes.FIELD("f2", DataTypes.STRING()))
            },
            row(Row.of("value1", 11)),
            "Column #2 with name 'row.f2' has type 'INT64' in BQ while in Flink it has type 'STRING'"));
  }
}

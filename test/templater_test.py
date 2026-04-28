"""Tests for the dataform templater."""
from pytest import mark



def test_replace_ref_with_bq_table_single_ref(templater):
    input_sql = "SELECT * FROM ${ref('test')}"
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_single_ref_double_quotes(templater):
    input_sql = 'SELECT * FROM ${ref("test")}'
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_with_dataset(templater):
    input_sql = "SELECT * FROM ${ref('other_dataset', 'test')}"
    expected_sql = "SELECT * FROM `my_project.other_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_with_dataset_double_quotes(templater):
    input_sql = 'SELECT * FROM ${ref("other_dataset", "test")}'
    expected_sql = "SELECT * FROM `my_project.other_dataset.test`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_ref_with_bq_table_multiple_refs(templater):
    input_sql = "SELECT * FROM ${ref('test')}, ${ref('another')}"
    expected_sql = "SELECT * FROM `my_project.my_dataset.test`, `my_project.my_dataset.another`"
    result = templater.replace_ref_with_bq_table(input_sql)
    assert result == expected_sql

def test_replace_self_with_bq_table(templater):
    input_sql = "SELECT * FROM ${self()}"
    expected_sql = "SELECT * FROM `my_project.my_dataset.self`"
    result = templater.replace_self_with_bq_table(input_sql)
    assert result == expected_sql


@mark.parametrize(
    "test_block, replaced_sql",
    [
        (
                "",
                ""
        ),
        (
                """config {
                type: "table",
                columns: {
                    "test" : "test",
                    "value:: "value"
                }
                }""",
                ""
        ),
        (
                """pre_operations {
                CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
                    RETURNS FLOAT64
                    AS ((x + 4) / y);
                }""",
                ""
        ),
        (
                """post_operations {
                GRANT `roles/bigquery.dataViewer`
                ON
                TABLE ${self()}
                TO "group:allusers@example.com", "user:otheruser@example.com"
                }""",
                ""
        ),
        (
                """config {
                type: "table",
                columns: {
                    "test" : "test",
                    "value:: "value"
                }
                } pre_operations {
                CREATE TEMP FUNCTION AddFourAndDivide(x INT64, y INT64)
                    RETURNS FLOAT64
                    AS ((x + 4) / y);
                } post_operations {
                GRANT `roles/bigquery.dataViewer`
                ON
                TABLE ${self()}
                TO "group:allusers@example.com", "user:otheruser@example.com"
                }""",
                "  "
        ),
    ]
)
def test_replace_blocks(templater, test_block, replaced_sql):
    sql_body = "SELECT * FROM my_table"
    input_sql = f"{test_block} {sql_body}"
    expected_sql = f"{replaced_sql} {sql_body}"
    result = templater.replace_blocks(input_sql)
    assert result == expected_sql


# slice_sqlx_template のテスト
def test_slice_sqlx_template_with_config_and_ref(templater):
    input_sqlx = """config {
    type: "table"
}
SELECT 1 AS value FROM ${ref('test')} WHERE true
"""
    expected_sql = "\nSELECT 1 AS value FROM `my_project.my_dataset.test` WHERE true\n"
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 4
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT 1")
    assert raw_slices[2].raw.startswith('${ref')
    assert raw_slices[3].raw == " WHERE true\n"

    assert len(templated_slices) == 4
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"

def test_slice_sqlx_template_with_multiple_refs(templater):
    input_sqlx = """config {
    type: "view",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM ${ref('test')}
JOIN ${ref('at')} ON test.id = at.id
JOIN ${ref('db', 'tb')} ON test.id = tb.id
JOIN ${ref('pc', 'dc', 'tc')} ON test.id = tc.id
JOIN ${ref({ name: 'at2' })} ON test.id = at2.id
JOIN ${ref({ schema: 'db2', name: 'tb2' })} ON test.id = tb2.id
JOIN ${ref({ database: constants.PROJECT_ID, schema: 'dc2', name: 'tc2' })} ON test.id = tc2.id
"""
    expected_sql = "".join(
            "\n"
            "SELECT * FROM `my_project.my_dataset.test`\n"
            "JOIN `my_project.my_dataset.at` ON test.id = at.id\n"
            "JOIN `my_project.db.tb` ON test.id = tb.id\n"
            "JOIN `pc.dc.tc` ON test.id = tc.id\n"
            "JOIN `my_project.my_dataset.at2` ON test.id = at2.id\n"
            "JOIN `my_project.db2.tb2` ON test.id = tb2.id\n"
            "JOIN `constants_PROJECT_ID.dc2.tc2` ON test.id = tc2.id\n"
            )

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    assert replaced_sql == expected_sql

    assert len(raw_slices) == 16
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith('${ref')
    assert raw_slices[3].raw.startswith('\nJOIN')
    assert raw_slices[4].raw.startswith('${ref')
    assert raw_slices[5].raw.endswith("ON test.id = at.id\nJOIN ")

    assert len(templated_slices) == 16
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"

def test_slice_sqlx_template_with_no_ref(templater):
    input_sqlx = """SELECT * FROM my_table WHERE true
"""
    expected_sql = "SELECT * FROM my_table WHERE true\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 1
    assert raw_slices[0].raw == "SELECT * FROM my_table WHERE true\n"

    assert len(templated_slices) == 1
    assert templated_slices[0].slice_type == "literal"


def test_slice_sqlx_template_with_js_block(templater):
    input_sqlx = """config {
    type: "table"
}
js {
    const a = 1;
}
SELECT 1 AS value FROM my_table WHERE true
"""
    expected_sql = """\n\nSELECT 1 AS value FROM my_table WHERE true
"""
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 4
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw == "\n"
    assert raw_slices[2].raw.strip().startswith("js")
    assert raw_slices[3].raw.startswith("\nSELECT 1")

    assert len(templated_slices) == 4
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"



def test_slice_sqlx_template_with_incremental_table(templater):
    input_sqlx = """config {
    type: "incremantal",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM ${ref('test')} JOIN ${ref('other_table')}
  ON test.id = other_table.id
${ when(incremantal(), "WHERE updated_at > '2020-01-01'")}
GROUP BY test
"""
    expected_sql = """
SELECT * FROM `my_project.my_dataset.test` JOIN `my_project.my_dataset.other_table`
  ON test.id = other_table.id

GROUP BY test
"""

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 8
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith('${ref')
    assert raw_slices[3].raw.startswith(' JOIN')
    assert raw_slices[4].raw.startswith('${ref')
    assert raw_slices[5].raw.endswith(" ON test.id = other_table.id\n")
    assert raw_slices[6].raw.startswith("${ when")
    assert raw_slices[7].raw.startswith("\nGROUP BY")

    assert len(templated_slices) == 8
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"
    assert templated_slices[6].slice_type == "templated"
    assert templated_slices[7].slice_type == "literal"


def test_slice_sqlx_template_with_post_operations_deeply_nested_js(templater):
    """Test slicing with post_operations containing deeply nested JS braces."""
    input_sqlx = """config {
    type: "table"
}
SELECT * FROM ${ref('test')}
post_operations {
  if (true) {
    const result = ${self()};
    if (result) {
      console.log('nested');
    }
  }
}
"""
    expected_sql = "\nSELECT * FROM `my_project.my_dataset.test`\n\n"
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    # Should have 6 slices: config, \nSELECT..., ${ref}, \n, post_operations, \n
    assert len(raw_slices) == 6
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw == "\nSELECT * FROM "
    assert raw_slices[2].raw == "${ref('test')}"
    assert raw_slices[3].raw == "\n"
    assert raw_slices[4].raw.startswith("post_operations")
    assert raw_slices[5].raw == "\n"

    assert len(templated_slices) == 6
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"


def test_slice_sqlx_template_with_post_operations_sql_block(templater):
    """Test slicing with post_operations containing SQL code (original bug report case)."""
    input_sqlx = """config {
    type: "table"
}
SELECT * FROM ${ref('test')}
post_operations {
  BEGIN
    ALTER TABLE ${self()} DROP PRIMARY KEY IF EXISTS;
    ALTER TABLE ${self()} ADD PRIMARY KEY (some_id, another_id) NOT ENFORCED;
  END;
}
"""
    expected_sql = "\nSELECT * FROM `my_project.my_dataset.test`\n\n"
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    # Should have 6 slices: config, \nSELECT..., ${ref}, \n, post_operations, \n
    assert len(raw_slices) == 6
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw == "\nSELECT * FROM "
    assert raw_slices[2].raw == "${ref('test')}"
    assert raw_slices[3].raw == "\n"
    assert raw_slices[4].raw.startswith("post_operations")
    assert raw_slices[5].raw == "\n"

    assert len(templated_slices) == 6
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"


def test_slice_sqlx_template_with_nested_js_expression(templater):
    """Test slicing with JavaScript expressions containing nested braces (location.sqlx case)."""
    input_sqlx = """SELECT
  CAST(code AS STRING) AS location_id,
  e.${ingestion.getTechnicalIngestionTimestamp({source: 'kafka'})} AS raw_source_ingestion_date_time,
  CURRENT_TIMESTAMP() AS raw_dataform_processing_date_time
FROM
  ${ ref({ name: "source_table_v1", schema: "kafka__raw__events" }) } AS e
"""
    expected_sql = """SELECT
  CAST(code AS STRING) AS location_id,
  e.js_expression AS raw_source_ingestion_date_time,
  CURRENT_TIMESTAMP() AS raw_dataform_processing_date_time
FROM
  `my_project.kafka__raw__events.source_table_v1` AS e
"""
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    # Should have 5 slices: literal, ${ingestion...}, literal, ${ref(...)}, literal
    assert len(raw_slices) == 5
    assert raw_slices[0].raw == "SELECT\n  CAST(code AS STRING) AS location_id,\n  e."
    assert raw_slices[1].raw == "${ingestion.getTechnicalIngestionTimestamp({source: 'kafka'})}"
    assert raw_slices[2].raw == " AS raw_source_ingestion_date_time,\n  CURRENT_TIMESTAMP() AS raw_dataform_processing_date_time\nFROM\n  "
    assert raw_slices[3].raw == '${ ref({ name: "source_table_v1", schema: "kafka__raw__events" }) }'
    assert raw_slices[4].raw == " AS e\n"

    assert len(templated_slices) == 5
    assert templated_slices[0].slice_type == "literal"
    assert templated_slices[1].slice_type == "templated"
    assert templated_slices[2].slice_type == "literal"
    assert templated_slices[3].slice_type == "templated"
    assert templated_slices[4].slice_type == "literal"


def test_slice_sqlx_template_with_when_expression_two_params(templater):
    """Test slicing with WHEN expression containing two parameters (work_item.sqlx case)."""
    input_sqlx = """SELECT
  item_id,
  rawSourceIngestionDateTime
FROM combined_items
WHERE
  rawSourceIngestionDateTime > ${when(incremental(), `event_timestamp_checkpoint`, `timestamp('0001-01-01')`) }

QUALIFY
  1 = ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY rawSourceIngestionDateTime DESC)
"""
    expected_sql = """SELECT
  item_id,
  rawSourceIngestionDateTime
FROM combined_items
WHERE
  rawSourceIngestionDateTime > `timestamp('0001-01-01')`

QUALIFY
  1 = ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY rawSourceIngestionDateTime DESC)
"""
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    # Should have 3 slices: literal, ${when...}, literal
    assert len(raw_slices) == 3
    assert raw_slices[0].raw == "SELECT\n  item_id,\n  rawSourceIngestionDateTime\nFROM combined_items\nWHERE\n  rawSourceIngestionDateTime > "
    assert raw_slices[1].raw == "${when(incremental(), `event_timestamp_checkpoint`, `timestamp('0001-01-01')`) }"
    assert raw_slices[2].raw == "\n\nQUALIFY\n  1 = ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY rawSourceIngestionDateTime DESC)\n"

    assert len(templated_slices) == 3
    assert templated_slices[0].slice_type == "literal"
    assert templated_slices[1].slice_type == "templated"
    assert templated_slices[2].slice_type == "literal"


def test_process_sqlx_with_config_and_ref(templater):
    input_sqlx = """config {
    type: "table",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM ${ref('test')} WHERE true
"""
    expected_sql = "\nSELECT * FROM `my_project.my_dataset.test` WHERE true\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 4
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith("${ref")
    assert raw_slices[3].raw == " WHERE true\n"

    assert len(templated_slices) == 4
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"

def test_process_sqlx_with_multiple_refs(templater):
    input_sqlx = """config {
    type: "view",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM ${ref('test')} JOIN ${ref('other_table')} ON test.id = other_table.id
"""
    expected_sql = "\nSELECT * FROM `my_project.my_dataset.test` JOIN `my_project.my_dataset.other_table` ON test.id = other_table.id\n"

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    assert replaced_sql == expected_sql

    assert len(raw_slices) == 6
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[1].raw.startswith("\nSELECT *")
    assert raw_slices[2].raw.startswith("${ref")
    assert raw_slices[3].raw.startswith(" JOIN")
    assert raw_slices[4].raw.startswith("${ref")
    assert raw_slices[5].raw.endswith(" ON test.id = other_table.id\n")

    assert len(templated_slices) == 6
    assert templated_slices[0].slice_type == "templated"
    assert templated_slices[1].slice_type == "literal"
    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[3].slice_type == "literal"
    assert templated_slices[4].slice_type == "templated"
    assert templated_slices[5].slice_type == "literal"


@mark.parametrize(
    "test_input_filename, expected",
    [
        (
            "config_pre_post_ref.sqlx",
            {
                "expected_sql": "\n\n\nSELECT * FROM `my_project.my_dataset.test` WHERE true\n",
                "raw_slices": {
                    "len": 8,
                    "raw_starts": [
                        "config",
                        "\n",
                        "pre_operations",
                        "\n",
                        "post_operations",
                        "\nSELECT *",
                        "${ref",
                        " WHERE true",
                    ]
                },
                "templated_slices": {
                    "len": 8,
                    "templated_types": [
                        "templated",
                        "literal",
                        "templated",
                        "literal",
                        "templated",
                        "literal",
                        "templated",
                        "literal",
                    ]
                }
            }

        )
    ]
)
def test_process_sqlx_with_post_pre_operations_config_and_ref(templater, test_inputs_dir_path, test_input_filename, expected):

    input_sqlx_path = test_inputs_dir_path / test_input_filename
    input_sqlx = input_sqlx_path.read_text()

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)

    assert replaced_sql ==  expected["expected_sql"]

    assert len(raw_slices) == expected["raw_slices"]["len"]
    for i, expected_raw_starts in enumerate(expected["raw_slices"]["raw_starts"]):
        assert raw_slices[i].raw.startswith(expected_raw_starts)

    assert len(templated_slices) == expected["templated_slices"]["len"]
    for i, expected_templated_types in enumerate(expected["templated_slices"]["templated_types"]):
        assert templated_slices[i].slice_type == expected_templated_types

def test_slice_sqlx_template_with_js_function_call(templater):
    input_sqlx = '''js {
  const { my_custom_function } = require("includes/my_includes");
}
CREATE OR REPLACE FUNCTION ${my_custom_function("standard_boolean_default")}(input BOOL, default_Value BOOL)
'''
    expected_sql = '''
CREATE OR REPLACE FUNCTION js_expression(input BOOL, default_Value BOOL)
'''
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql.strip() == expected_sql.strip()

def test_slice_sqlx_template_with_mixed_ordering(templater):
    """Test ensuring patterns appearing earlier in text but later in priority list are captured correctly."""
    input_sqlx = """config { type: "view" }
SELECT ${my_var}, ${ref('my_table')}
"""
    expected_sql = '\nSELECT js_expression, `my_project.my_dataset.my_table`\n'

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    assert len(raw_slices) == 6
    assert raw_slices[0].slice_type == "templated"  # config
    assert raw_slices[1].slice_type == "literal"    # SELECT
    assert raw_slices[2].slice_type == "templated"  # ${my_var}
    assert raw_slices[2].raw == "${my_var}"
    assert raw_slices[3].slice_type == "literal"    # ,
    assert raw_slices[4].slice_type == "templated"  # ${ref...}
    assert raw_slices[5].slice_type == "literal"    # \n


def test_slice_sqlx_template_with_self_and_js_expression(templater):
    """Test ensuring both ${self()} and regular JS expressions are handled in the same file."""
    input_sqlx = """config { type: "table" }
SELECT ${column_name} FROM ${self()}
"""
    expected_sql = '\nSELECT js_expression FROM `my_project.my_dataset.self`\n'

    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql

    # Slices: config(0), \nSELECT (1), ${column_name}(2),  FROM (3), ${self()}(4), \n(5)
    assert len(raw_slices) == 6
    assert raw_slices[0].raw.startswith("config")
    assert raw_slices[2].raw == "${column_name}"
    assert raw_slices[4].raw == "${self()}"

    assert templated_slices[2].slice_type == "templated"
    assert templated_slices[4].slice_type == "templated"


def test_slice_sqlx_template_with_when_containing_nested_self(templater):
    """${when()} block whose body contains ${self()} should be sliced correctly.

    The non-greedy regex ``\\$\\{\\s*when\\((.*?)\\)\\s*\\}`` would otherwise
    close at the inner ``${self()}``'s ``)}``, undercounting the templated
    slice length and producing "Length of templated file mismatch with final
    slice" during lint.
    """
    input_sqlx = """SELECT 1 AS x
${when(incremental(),
    `WHERE updated_at > (SELECT MAX(t) FROM ${self()})`)}
"""
    expected_sql = "SELECT 1 AS x\n\n"
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql
    # Final slice's stop must equal the replaced length.
    assert templated_slices[-1].templated_slice.stop == len(replaced_sql)


def test_slice_sqlx_template_with_multiline_ref(templater):
    """``${\\n  ref(...)\\n}`` should match across newlines.

    Without DOTALL the regex fails to match, the global pass falls through to
    ``replace_js_expressions`` which substitutes ``js_expression``, but the
    slicing dispatch sees ``ref(`` in the raw match and returns the unchanged
    block. The resulting slice length disagrees with the replaced length.
    """
    input_sqlx = """SELECT * FROM ${
  ref({
    schema: "checkmate_events",
    name: "extension_installed"
  })
} e
"""
    expected_sql = (
        "SELECT * FROM `my_project.checkmate_events.extension_installed` e\n"
    )
    replaced_sql, raw_slices, templated_slices = templater.slice_sqlx_template(input_sqlx)
    assert replaced_sql == expected_sql
    assert templated_slices[-1].templated_slice.stop == len(replaced_sql)



def test_has_js_block(templater):
    has_js_sql = """config {
    type: "table",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
js {
    const myVar = "test";
}
SELECT * FROM my_table"""
    not_has_js_sql = """config {
    type: "table",
    columns: {
        "test" : "test",
        "value:: "value"
    }
}
SELECT * FROM my_table"""
    assert templater.has_js_block(has_js_sql)
    assert not templater.has_js_block(not_has_js_sql)

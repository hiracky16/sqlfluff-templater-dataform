# Dataform plugin for SQLFluff

This project is based on [dbt plugin for SQLFluff](https://github.com/hiracky16/sqlfluff/blob/main/plugins/sqlfluff-templater-dbt/) and is licensed under the MIT License.

This plugin works with [SQLFluff](https://pypi.org/project/sqlfluff/), the
SQL linter for humans, to correctly parse and compile SQL projects using
[Dataform](https://cloud.google.com/dataform).


## SQLFluff configuration

As dataform uses mainly '.sqlx' files you will need to set the '.sqlfluff' as below:

```
[sqlfluff]
templater = dataform
dialect = bigquery
sql_file_exts = .sql,.sqlx
```
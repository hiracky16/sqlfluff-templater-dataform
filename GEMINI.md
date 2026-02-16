# Dataform plugin for SQLFluff

## Abstract
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

## development（for Docker）
### 開発ワークフロー
1.  **問題の理解と計画**: 変更を加える前に、問題（バグ修正、機能追加など）を深く理解し、既存のコードベース（周囲のコード、テスト、設定ファイルなど）を分析します。これにより、プロジェクトの命名規則、コーディングスタイル、構造を尊重した変更を計画できます。
2.  **実装**: 計画に基づきコードを実装します。常に既存の慣例に従い、プロジェクトの全体的な整合性を保ちます。
3.  **テスト**: 変更が正しく機能し、既存の機能に影響を与えないことを確認するために、テスト（新しいテストケースの追加や既存のテストの修正）を行います。特に、Docker環境を活用してテストを実行することを推奨します。
4.  **バージョン管理**: 変更は適切な粒度でコミットし、明確なコミットメッセージを記述します。最終的にはプルリクエストを作成してコードレビューを受けます。

### Dockerを使用した開発
ローカル環境を汚さずに開発・テストを行うため、Dockerの使用を強く推奨します。

#### ビルド
```
cd docker/
docker compose up -d
```

#### テスト
Dockerコンテナ内でテストを実行するには、以下のコマンドを使用します。
```sh
docker compose exec app pytest
```

#### アドホックテスト（特定のルールやファイル）
```sh
docker compose exec app sqlfluff lint test/fixture/dataform
```

## Release

リリースプロセスには、Gitタグの作成とプッシュ、そしてPyPIへのパッケージのビルドとアップロードが含まれます。
main ブランチで作業を行うこと。

### 1. Increment version
pyproject.toml のバージョンを一つ上げる。
基本的にパッチバージョンを上げる。

### 2. Tagging

```sh
$ git checkout main && git pull origin main
$ git tag
$ git tag v0.1.7 # increment latest version
$ git push origin v0.1.7
```

### 3. Build and Upload

以下のコマンドで実施可能
※ Tagをpushした段階でGitHub Actionsが実行してくれるので不要

```sh
# パッケージのビルド
python -m build

# PyPIへのアップロード
python -m twine upload dist/*
```

### 4. Create release note
GitHub 上でリリースノートを作成する。

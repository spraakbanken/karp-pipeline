from typing import Generator, Iterable


from karppipeline.common import PipelineException, get_output_dir
from karppipeline.modules.karps.models import KarpsExportConfig, TableMetadata, save_table_metadata
from karppipeline.models import Entry, EntrySchema, PipelineConfig, InferredField

VARCHAR_CUTOFF = 400  # if a field contains values larger than this, use TEXT type and skip indexing


def create_karps_sql(
    pipeline_config: PipelineConfig, karps_config: KarpsExportConfig, resource_config: EntrySchema
) -> Generator[None, Entry | None, None]:
    def schema(table_name: str, structure: EntrySchema) -> tuple[str, str, str]:
        """
        Find schema automatically by going through all elements
        """

        def create_delete_statement(table_name) -> str:
            """
            Each resource with collections produces multiple tables, prefixed with {resource_id}__ and these statements
            removed them dynamically
            """
            return f"""

            SELECT CONCAT('DROP TABLE IF EXISTS `', GROUP_CONCAT(TABLE_NAME SEPARATOR '`, `'), '`;')
            INTO @drop_stmt FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE '{table_name}__%';
            SET @run_stmt = IF(@drop_stmt IS NOT NULL, @drop_stmt, 'SELECT "No tables to drop";');
            PREPARE stmt FROM @run_stmt;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            DROP TABLE IF EXISTS `{table_name}`;
            """

        def inner(_structure: Iterable[InferredField]):
            tables = []
            fields = []
            indices = []
            for field in _structure:
                field_name = field.name
                if field.collection:
                    if field.type == "table":
                        columns = field.fields
                    else:
                        columns = {field_name: field}
                    # same but not collection
                    table_fields = (
                        InferredField(name=val.name, type=val.type, collection=False, extra=val.extra)
                        for val in columns.values()
                    )
                    _, inner_fields, _ = inner(table_fields)
                    inner_table_name = f"{table_name}__{field_name}"
                    tables.append(f"""
                    CREATE TABLE `{inner_table_name}` (
                        {",\n".join(inner_fields)},
                        __parent_id INT,
                        FOREIGN KEY (__parent_id) REFERENCES `{table_name}`(__id)
                    )
                    CHARACTER SET {karps_config.db_charset}
                    COLLATE {karps_config.db_collation};
                    """)

                    for idx, (col_name, inner_field) in enumerate(columns.items()):
                        if inner_field.type == "text" and inner_field.length <= VARCHAR_CUTOFF:
                            indices.append(
                                # TODO add __id
                                f"CREATE INDEX `{inner_table_name[0:55]}_{idx}_idx` ON `{inner_table_name}`(`{col_name}`({inner_field.extra['length']}));"
                            )
                else:
                    if field.type == "integer":
                        column_type = "INT"
                    elif field.type == "text":
                        if field.length > VARCHAR_CUTOFF:
                            column_type = "TEXT"
                        else:
                            column_type = f"VARCHAR({field.extra['length']})"
                            indices.append(
                                f"CREATE INDEX `{(table_name + '__' + field_name)[0:60]}_idx` ON `{table_name}`(`{field_name}`({field.extra['length']}));"
                            )
                    elif field.type == "float":
                        column_type = "FLOAT"
                    elif field.type == "bool":
                        column_type = "BOOLEAN"
                    else:
                        raise PipelineException(f"unknown column type in schema: {field.type}")
                    fields.append(f"`{field_name}` {column_type}")
            return tables, fields, indices

        tables, fields, indices = inner(structure.values())

        delete_stmt = create_delete_statement(table_name)
        return (
            (
                f"""
        CREATE TABLE `{table_name}` (
            __id INT PRIMARY KEY,
            {",\n".join(fields)}
        )
        CHARACTER SET {karps_config.db_charset}
        COLLATE {karps_config.db_collation};
        """
                + "".join(tables)
            ),
            "\n".join(indices) + "\n",
            delete_stmt,
        )

    def entries_sql() -> Generator[tuple[dict[str, list[str]], str], Entry | None, None]:
        idx = 0
        join_rows, row = {}, ""
        while True:
            entry = yield join_rows, row
            if entry is None:
                break

            def format_str(val):
                """
                escape backslashes, tabs and newlines
                """
                return val.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")

            def format_value(val):
                if val is None:
                    return "NULL"
                elif isinstance(val, str):
                    return format_str(val)
                elif isinstance(val, int) or isinstance(val, float):
                    return str(val)
                elif isinstance(val, dict):
                    return "\t".join([format_value(v) for v in val.values()])
                else:
                    raise PipelineException(f"unknown column type  in data: {type(val)}")

            def sqlify_values(entry):
                """
                if values are scalar, they must be formatted/encoded in a wway that makes sense for MySQL
                if values are lists, they must be transformed into a separate INSERT statement with a ref to parent (idx from closure)
                """
                inserts = {}
                main_values = [str(idx)]
                for field_name, val in entry.items():
                    if isinstance(val, list):
                        for x in val:
                            if field_name not in inserts:
                                inserts[field_name] = []

                            inserts[field_name].append("\t".join([str(idx), format_value(x)]))
                    elif val is not None:
                        main_values.append(format_value(val))

                return inserts, "\t".join(main_values)

            join_rows, row = sqlify_values(entry)

            idx += 1

    tables_metadata = []  # collects the information needed to run each csv file
    sql_gen = entries_sql()
    next(sql_gen)
    schema_sql, indices, delete_statement = schema(pipeline_config.resource_id, resource_config)

    path = get_output_dir(pipeline_config.workdir) / "karps"
    main_csv_path = path / f"{pipeline_config.resource_id}.csv"
    columns = [field.name for field in resource_config.values() if not field.collection]
    tables_metadata = [
        TableMetadata(csv_file=main_csv_path, table=pipeline_config.resource_id, columns=("__id", *columns))
    ]

    join_table_files = {}
    for field_name, field in resource_config.items():
        if field.collection:
            csv_path = path / f"{pipeline_config.resource_id}__{field_name}.csv"
            fp = open(csv_path, "w")
            join_table_files[field_name] = fp
            tables_metadata.append(
                TableMetadata(
                    csv_file=csv_path,
                    table=f"{pipeline_config.resource_id}__{field_name}",
                    columns=["__parent_id", field_name],
                )
            )

    save_table_metadata(pipeline_config, tables_metadata)

    with open(path / "create.sql", "w") as fp:
        fp.write("SET SESSION max_statement_time = 0;")
        fp.write(delete_statement)
        fp.write(schema_sql)

    with open(path / "indices.sql", "w") as fp:
        fp.write("SET SESSION max_statement_time = 0;")
        fp.write(indices)

    # one file to delete resource, used in uninstall
    with open(get_output_dir(pipeline_config.workdir) / "karps/delete.sql", "w") as fp:
        fp.write("SET SESSION max_statement_time = 0;")
        fp.write(delete_statement)

    with open(main_csv_path, "w") as fp:
        while True:
            entry = yield
            if not entry:
                break
            join_lines, line = sql_gen.send(entry)
            fp.write(line)
            fp.write("\n")
            for field_name, lines in join_lines.items():
                for line in lines:
                    join_table_files[field_name].write(line)
                    join_table_files[field_name].write("\n")

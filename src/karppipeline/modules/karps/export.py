import time
from typing import Generator, Iterable, Iterator, Mapping


from karppipeline.common import create_output_dir, get_output_dir
from karppipeline.modules.karps.models import KarpsConfig
from karppipeline.models import Entry, EntrySchema, PipelineConfig, InferredField
from karppipeline.util import yaml

VARCHAR_CUTOFF = 200  # if a field contains values larger than this, use TEXT type and skip indexing


def create_karps_backend_config(
    pipeline_config: PipelineConfig,
    karps_config: KarpsConfig,
    name: dict[str, str],
    entry_schema: EntrySchema,
    source_order: list[str],
    size: int,
    fields: list[dict[str, str]],
):
    # these fields might already be present in backend config, install must merge this file and backend fields.yaml
    with open(create_output_dir(pipeline_config.workdir) / "fields.yaml", "w") as fp:
        yaml.dump(fields, fp)

    def order_fields(fields: Iterator[str]) -> Iterable[str]:
        # initialize main sort order
        order_map = {name: i for i, name in enumerate([field.name for field in pipeline_config.fields])}

        # order by apperance in input objects for non-configured fields
        for i, name in enumerate(source_order):
            if name not in order_map:
                order_map[name] = len(pipeline_config.fields) + i

        # should be no unknown fields at this point
        sorted_keys = sorted(fields, key=lambda x: order_map[x])
        return sorted_keys

    def make_field_config(fields: Iterable[str]) -> Iterator[Mapping[str, object]]:
        """
        creates the final format for a field in karps config
        if only one of karps.primary/secondary is given:
            for each key in karps_config.primary, add primary: true and primary: false to the rest
            for each key in karps_config.secondary, add primary: false and primary: true to the rest
        else:
            add primary: true/false as expected and raise error if a field is not in either
        """
        primary = karps_config.primary
        secondary = karps_config.secondary
        for field in fields:
            if primary and secondary:
                if not (field in primary or field in secondary):
                    raise Exception(
                        f'Karps: field {field} has to be in either primary or secondary. Use "not {field}" in export.fields to exclude field or update primary/secondary.'
                    )
                is_primary = field in primary
            elif karps_config.primary:
                is_primary = field in primary
            elif karps_config.secondary:
                is_primary = field not in secondary
            else:
                # if primary/secondary is not configured, all fields are primary
                is_primary = True
            yield {"name": field, "primary": is_primary}

    final_field_list = order_fields(iter(entry_schema.keys()))
    backend_config = {
        "resource_id": pipeline_config.resource_id,
        "label": name or pipeline_config.resource_id,
        "fields": list(make_field_config(final_field_list)),
        "entry_word": karps_config.entry_word.model_dump(),
        "size": size,
        "link": karps_config.link,
        "updated": int(time.time()),
    }
    if karps_config.entry_word.field not in final_field_list:
        raise ImportError(f"entry_word: {karps_config.entry_word.field}, but field is not available in the resource")
    if karps_config.tags:
        backend_config["tags"] = karps_config.tags
    if pipeline_config.description:
        backend_config["description"] = pipeline_config.description.model_dump()
    if pipeline_config.limited_access:
        backend_config["limited_access"] = pipeline_config.limited_access

    with open(get_output_dir(pipeline_config.workdir) / f"{pipeline_config.resource_id}_karps.yaml", "w") as fp:
        yaml.dump(backend_config, fp)


def create_karps_sql(
    pipeline_config: PipelineConfig, karps_config: KarpsConfig, resource_config: EntrySchema
) -> Generator[None, Entry | None, None]:
    def schema(table_name: str, structure: EntrySchema) -> tuple[str, str]:
        """
        Find schema automatically by going through all elements
        """

        def delete_statement(table_name) -> str:
            """
            Each resource with collections produces multiple tables, prefixed with {resource_id}__ and these statements
            removed them dynamically
            """
            return f"""
            SELECT CONCAT('DROP TABLE IF EXISTS `', GROUP_CONCAT(TABLE_NAME SEPARATOR '`, `'), '`;')
            INTO @drop_stmt FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = '{karps_config.db_database}' AND TABLE_NAME LIKE '{table_name}__%';
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
                        columns = {field.name: field}
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

                    for col_name, inner_field in columns.items():
                        if inner_field.type == "text" and inner_field.length <= VARCHAR_CUTOFF:
                            indices.append(
                                f"CREATE INDEX `{inner_table_name}_{col_name}_idx` ON `{inner_table_name}`(`{col_name}`({inner_field.extra['length']}));"
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
                                f"CREATE INDEX `{table_name}__{field_name}_idx` ON `{table_name}`(`{field_name}`({field.extra['length']}));"
                            )
                    elif field.type == "float":
                        column_type = "FLOAT"
                    else:
                        raise Exception("unknown column type", field.type)
                    fields.append(f"`{field_name}` {column_type}")
            return tables, fields, indices

        tables, fields, indices = inner(structure.values())

        return (
            f"""
        {delete_statement(table_name)}
        CREATE TABLE `{table_name}` (
            __id INT PRIMARY KEY,
            {",\n".join(fields)}
        )
        CHARACTER SET {karps_config.db_charset}
        COLLATE {karps_config.db_collation};
        """
            + "".join(tables)
        ), "\n".join(indices) + "\n"

    def entries_sql() -> Generator[list[str], Entry | None, None]:
        idx = 0
        lines = []
        while True:
            entry = yield lines
            if entry is None:
                break

            def format_str(val):
                """
                Wrap string in single quotes, escape backslashes and single quotes
                """
                return f"'{val.replace('\\', '\\\\').replace("'", "\\'").replace('\n', '\\n')}'"

            def format_value(val):
                if val is None:
                    return "NULL"
                elif isinstance(val, str):
                    return format_str(val)
                elif isinstance(val, int) or isinstance(val, float):
                    return str(val)
                elif isinstance(val, dict):
                    return ",".join([format_value(v) for v in val.values()])
                else:
                    raise Exception("unknown type")

            def sqlify_values(entry):
                """
                if values are scalar, they must be formatted/encoded in a wway that makes sense for MySQL
                if values are lists, they must be transformed into a separate INSERT statement with a ref to parent (idx from closure)
                """
                inserts = []
                columns = []
                main_values = []
                for field_name, val in entry.items():
                    if isinstance(val, list):
                        for x in val:
                            if isinstance(x, dict):
                                keys = x.keys()
                            else:
                                keys = [field_name]
                            inserts.append(
                                f"INSERT INTO `{pipeline_config.resource_id}__{field_name}` (__parent_id, {','.join(f'`{key}`' for key in keys)}) VALUES ({idx}, {format_value(x)});\n"
                            )
                    elif val is not None:
                        columns.append(field_name)
                        main_values.append(format_value(val))
                return inserts, columns, main_values

            inserts, columns, values = sqlify_values(entry)

            # main entry
            lines = [
                f"INSERT INTO `{pipeline_config.resource_id}` (`__id`, {', '.join(f'`{column}`' for column in columns)}) VALUES ({idx}, {', '.join(values)});\n"
            ] + inserts

            idx += 1

    sql_gen = entries_sql()
    next(sql_gen)
    with open(get_output_dir(pipeline_config.workdir) / f"{pipeline_config.resource_id}.sql", "w") as fp:
        schema_sql, indices = schema(pipeline_config.resource_id, resource_config)
        fp.write(schema_sql)
        fp.write(indices)
        while True:
            entry = yield
            if not entry:
                # TODO it this needed or can sql_gen be killed/gc:ed when the outer generator is done
                sql_gen.send(None)
                break
            for line in sql_gen.send(entry):
                fp.write(line)

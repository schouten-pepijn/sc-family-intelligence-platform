from __future__ import annotations

import fip.cli as cli


@cli.app.command("inspect-cbs-raw")
def inspect_cbs_raw(
    table_id: str = cli.typer.Option("83625NED", help="CBS table id to inspect."),
    run_id: str = cli.typer.Option("debug-raw", help="Run id for the inspection session."),
    limit: int = cli.typer.Option(5, help="Number of records to print per entity."),
    entity: str | None = cli.typer.Option(
        None,
        help="Optional entity filter: Observations, MeasureCodes, PeriodenCodes, or RegioSCodes.",
    ),
) -> None:
    source = cli.CBSODataSource(table_id=table_id, run_id=run_id)

    for record in cli.iter_sampled_cbs_raw_records(source=source, entity=entity, limit=limit):
        cli.typer.echo(record.entity_name)
        cli.typer.echo(f"natural_key={record.natural_key}")
        cli.typer.echo(cli.json.dumps(record.payload, indent=2, ensure_ascii=False))
        cli.typer.echo("")


@cli.app.command("inspect-bag-raw")
def inspect_bag_raw(
    run_id: str = cli.typer.Option("debug-raw", help="Run id for the inspection session."),
    collection: str = cli.typer.Option(
        "verblijfsobject",
        help="BAG collection to inspect, for example verblijfsobject or pand.",
    ),
    limit: int = cli.typer.Option(5, help="Number of records to print."),
    entity: str | None = cli.typer.Option(
        None,
        help="Optional entity filter, for example verblijfsobject or pand.",
    ),
) -> None:
    source = cli.PDOKBAGSource(run_id=run_id, collection=collection)
    for record in cli.iter_sampled_bag_raw_records(source=source, entity=entity, limit=limit):
        cli.typer.echo(record.entity_name)
        cli.typer.echo(f"natural_key={record.natural_key}")
        cli.typer.echo(cli.json.dumps(record.payload, indent=2, ensure_ascii=False))
        cli.typer.echo("")


@cli.app.command("inspect-bronze")
def inspect_bronze(
    table_name: str = cli.typer.Option(..., "--table", help="Bronze table name to inspect."),
    namespace: str | None = cli.typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured bronze namespace.",
    ),
    limit: int = cli.typer.Option(5, help="Number of sample rows to display."),
) -> None:
    con = cli.connect_duckdb()

    try:
        cli.load_extensions(con)
        cli.attach_lakekeeper_catalog(con)

        row_count = cli.count_rows(con, table_name=table_name, namespace=namespace)
        rows = cli.sample_rows(con, table_name=table_name, namespace=namespace, limit=limit)
    finally:
        con.close()

    cli.typer.echo(f"Row count: {row_count}")
    cli.typer.echo(f"Sample rows ({len(rows)}):")
    for row in rows:
        cli.typer.echo(str(row))


@cli.app.command("inspect-bag-bronze")
def inspect_bag_bronze(
    collection: str = cli.typer.Option(
        "verblijfsobject",
        help="BAG collection to inspect, for example verblijfsobject or pand.",
    ),
    namespace: str | None = cli.typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured bronze namespace.",
    ),
    limit: int = cli.typer.Option(5, help="Number of sample rows to display."),
) -> None:
    inspect_bronze(
        table_name=f"bag_{collection}",
        namespace=namespace,
        limit=limit,
    )


@cli.app.command("inspect-cbs-silver")
def inspect_cbs_silver(
    table_name: str = cli.typer.Option(
        "cbs_observations_flat_83625ned",
        "--table",
        help="Silver table name to inspect.",
    ),
    namespace: str | None = cli.typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured silver namespace.",
    ),
    limit: int = cli.typer.Option(5, help="Number of sample rows to display."),
) -> None:
    silver_namespace = namespace or cli.get_settings().silver_namespace
    con = cli.connect_duckdb()

    try:
        cli.load_extensions(con)
        cli.attach_lakekeeper_catalog(con)

        row_count = cli.count_rows(con, table_name=table_name, namespace=silver_namespace)
        rows = cli.sample_rows(con, table_name=table_name, namespace=silver_namespace, limit=limit)
    finally:
        con.close()

    cli.typer.echo(f"Row count: {row_count}")
    cli.typer.echo(f"Sample rows ({len(rows)}):")
    for row in rows:
        cli.typer.echo(str(row))


@cli.app.command("inspect-bag-silver")
def inspect_bag_silver(
    namespace: str | None = cli.typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured silver namespace.",
    ),
    limit: int = cli.typer.Option(5, help="Number of sample rows to display."),
) -> None:
    inspect_cbs_silver(
        table_name="bag_verblijfsobject_flat",
        namespace=namespace,
        limit=limit,
    )


@cli.app.command("inspect-bag-silver-pand")
def inspect_bag_silver_pand(
    namespace: str | None = cli.typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured silver namespace.",
    ),
    limit: int = cli.typer.Option(5, help="Number of sample rows to display."),
) -> None:
    inspect_cbs_silver(
        table_name="bag_pand_flat",
        namespace=namespace,
        limit=limit,
    )


@cli.app.command("inspect-bag-landing-verblijfsobject")
def inspect_bag_landing_verblijfsobject(
    schema: str | None = cli.typer.Option(
        None,
        help="Postgres schema to inspect. Defaults to configured landing schema.",
    ),
    limit: int = cli.typer.Option(5, help="Number of sample rows to display."),
) -> None:
    inspect_landing(
        table_name="bag_verblijfsobject",
        schema=schema,
        limit=limit,
    )


@cli.app.command("inspect-bag-landing-pand")
def inspect_bag_landing_pand(
    schema: str | None = cli.typer.Option(
        None,
        help="Postgres schema to inspect. Defaults to configured landing schema.",
    ),
    limit: int = cli.typer.Option(5, help="Number of sample rows to display."),
) -> None:
    inspect_landing(
        table_name="bag_pand",
        schema=schema,
        limit=limit,
    )


@cli.app.command("inspect-landing")
def inspect_landing(
    table_name: str = cli.typer.Option(
        "cbs_observations",
        "--table",
        help="Gold table name to inspect.",
    ),
    schema: str | None = cli.typer.Option(
        None,
        help="Postgres schema to inspect. Defaults to configured landing schema.",
    ),
    limit: int = cli.typer.Option(5, help="Number of sample rows to display."),
) -> None:
    conn = cli.connect_postgres()

    try:
        row_count = cli.count_gold_rows(conn, table_name=table_name, schema=schema)
        rows = cli.sample_gold_rows(conn, table_name=table_name, schema=schema, limit=limit)
    finally:
        conn.close()

    cli.typer.echo(f"Row count: {row_count}")
    cli.typer.echo(f"Sample rows ({len(rows)}):")
    for row in rows:
        cli.typer.echo(str(row))

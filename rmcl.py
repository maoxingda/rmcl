import json
import os
import re
import textwrap
from pprint import pprint

from datetime import datetime, timedelta, timezone

import click
import psycopg2
import pyperclip
from jinja2 import Template


def remove_comments(text):
    # /*
    # ...
    # */
    multiline_comment_pattern = re.compile(r'''
        (?<=/\*) # start with /*
        (?:.*?)  # non-capture group & any chars, contains newline(\n), non-greedy mode
        (?=\*/)  # end with */
    ''', re.VERBOSE | re.DOTALL)

    # -- ...
    single_line_pattern = re.compile(r'''
        -- # start
        .* # any chars, not contains newline(\n)
    ''', re.VERBOSE)

    text = multiline_comment_pattern.sub('', text)
    text = single_line_pattern.sub('', text)

    return text


@click.group()
def main():
    pass


@main.command()
@click.option('-r', '--render/--no-render', default=False)
@click.option('-c', '--client-snowflake-id', required=True, prompt='客户雪花ID')
@click.option('-a', '--balance-account-snowflake-id', required=True, prompt='个人余额账户ID')
@click.option('-s', '--start-date', required=True, prompt='报表查询开始日期')
@click.option('-e', '--end-date', required=True, prompt='报表查询截止日期')
@click.argument('file_name', required=True, type=click.Path(exists=True))
def a7(
        render,
        client_snowflake_id,
        balance_account_snowflake_id,
        start_date,
        end_date,
        file_name,
):
    work_dir = os.getcwd()
    sql_file_path = f'{work_dir}/{file_name}'

    if render:
        with open(sql_file_path) as f:
            sql = Template(f.read()).render({
                'client_snowflake_id': client_snowflake_id,
                'balance_account_snowflake_id': balance_account_snowflake_id,
                'date_start': start_date,
                'date_end': end_date,
            })

        with open(sql_file_path, 'w') as f:
            f.write(sql)
    else:
        with open(sql_file_path) as f:
            sql = f.read().replace(client_snowflake_id, '{{client_snowflake_id}}')
            sql = sql.replace(balance_account_snowflake_id, '{{balance_account_snowflake_id}}')
            sql = sql.replace(start_date, '{{date_start}}')
            sql = sql.replace(end_date, '{{date_end}}')

        with open(sql_file_path, 'w') as f:
            f.write(sql)


@main.command()
@click.option('-r', '--render/--no-render', default=False)
@click.option('-c', '--client-snowflake-id', required=True, prompt='客户雪花ID')
@click.option('-a', '--wallet-id', required=True, prompt='第三方钱包账户ID')
@click.option('-s', '--start-date', required=True, prompt='报表查询开始日期')
@click.option('-e', '--end-date', required=True, prompt='报表查询截止日期')
@click.argument('file_name', required=True, type=click.Path(exists=True))
def t1(
        render,
        client_snowflake_id,
        wallet_id,
        start_date,
        end_date,
        file_name,
):
    work_dir = os.getcwd()
    sql_file_path = f'{work_dir}/{file_name}'

    if render:
        with open(sql_file_path) as f:
            sql = Template(f.read()).render({
                'client_snowflake_id': client_snowflake_id,
                'wallet_id': wallet_id,
                'date_start': start_date,
                'date_end': end_date,
            })

        with open(sql_file_path, 'w') as f:
            f.write(sql)
    else:
        with open(sql_file_path) as f:
            sql = f.read().replace(client_snowflake_id, '{{client_snowflake_id}}')
            sql = sql.replace(wallet_id, '{{wallet_id}}')
            sql = sql.replace(start_date, '{{date_start}}')
            sql = sql.replace(end_date, '{{date_end}}')

        with open(sql_file_path, 'w') as f:
            f.write(sql)


@main.command()
@click.option('-r', '--render/--no-render', default=False)
@click.option('-c', '--client-snowflake-id', default='abcdefhjkl')
@click.option('-s', '--subsidy-snowflake-id', required=True, prompt='餐补雪花ID')
@click.option('-s', '--start-date', required=True, prompt='报表查询开始日期')
@click.option('-e', '--end-date', required=True, prompt='报表查询截止日期')
@click.argument('file_name', required=True, type=click.Path(exists=True))
def s6(
        render,
        client_snowflake_id,
        subsidy_snowflake_id,
        start_date,
        end_date,
        file_name,
):
    work_dir = os.getcwd()
    sql_file_path = f'{work_dir}/{file_name}'

    if render:
        with open(sql_file_path) as f:
            sql = Template(f.read()).render({
                'client_snowflake_id': client_snowflake_id,
                'subsidy_snowflake_id': subsidy_snowflake_id,
                'date_start': start_date,
                'date_end': end_date,
            })

        with open(sql_file_path, 'w') as f:
            f.write(sql + '\n')
    else:
        with open(sql_file_path) as f:
            sql = f.read().replace(client_snowflake_id, '{{client_snowflake_id}}')
            sql = sql.replace(subsidy_snowflake_id, '{{subsidy_snowflake_id}}')
            sql = sql.replace(start_date, '{{date_start}}')
            sql = sql.replace(end_date, '{{date_end}}')

        with open(sql_file_path, 'w') as f:
            f.write(sql)


@main.command()
@click.option('-r', '--render/--no-render', default=False)
@click.option('-s', '--client-member-sn-id', required=True, prompt='客户成员雪花ID')
@click.option('-s', '--start-date', required=True, prompt='报表查询开始日期')
@click.option('-e', '--end-date', required=True, prompt='报表查询截止日期')
@click.argument('file_name', required=True, type=click.Path(exists=True))
def r15(
        render,
        client_member_sn_id,
        start_date,
        end_date,
        file_name,
):
    work_dir = os.getcwd()
    sql_file_path = f'{work_dir}/{file_name}'

    if render:
        with open(sql_file_path) as f:
            sql = Template(f.read()).render({
                'client_member_sn_id': client_member_sn_id,
                'date_start': start_date,
                'date_end': end_date,
            })

        with open(sql_file_path, 'w') as f:
            f.write(sql)
    else:
        with open(sql_file_path) as f:
            sql = f.read().replace(client_member_sn_id, '{{client_member_sn_id}}')
            sql = sql.replace(start_date, '{{date_start}}')
            sql = sql.replace(end_date, '{{date_end}}')

        with open(sql_file_path, 'w') as f:
            f.write(sql)


@main.command()
@click.option('-s', '--schemas', show_default=True, default='stg|dim|dwd|dws|met')
@click.option('-f', '--filters', show_default=True, default='_stg|_staging|_tmp|_temp')
@click.option('-e', '--exist / --no-exist', default=False)
@click.argument('file_name', required=True, type=click.Path(exists=True))
def depends(
        schemas,
        filters,
        exist,
        file_name,
):
    # <schema>.<table>
    schema_table_pattern = re.compile(rf'''
        \b            # word boundary
        (?:{schemas}) # schema, non-capture group
        \.            # .
        \w+           # table
    ''', re.VERBOSE)

    work_dir = os.getcwd()
    sql_file_path = os.path.join(work_dir, file_name)

    with open(sql_file_path) as f:
        sql = f.read()

    sql = remove_comments(sql)
    tables = schema_table_pattern.findall(sql)

    def is_exist(table):
        schema_name = table.split('.')[0]
        table_name = table.split('.')[1]
        with psycopg2.connect(
                f'postgresql://{os.environ.get("REDSHIFT_SANDBOX")}') as conn:
            with conn.cursor() as cursor:
                sql = f'set search_path to {schema_name};'
                cursor.execute(sql)
                sql = f"""
                    select 1
                    from svv_all_tables
                    where schema_name = '{schema_name}' and table_name = '{table_name}'
                """
                cursor.execute(sql)
                if cursor.fetchall():
                    return True

    tables = set(
        schema_table for schema_table in tables
        if schema_table.split('.')[1] not in file_name and (not exist or is_exist(schema_table))
    )

    tables = [
        schema_table for schema_table in tables if not re.search(rf'{filters}', schema_table.split('.')[1])
    ]

    pprint(sorted(tables))


@main.command()
@click.option('-t', '--table-name', required=True)
@click.option('-f', '--filter', multiple=True)
@click.argument('directory', default='.', type=click.Path(exists=True))
def downstream(
        table_name,
        directory,
        filter
):
    if not re.match(r'\w+\.\w+', table_name):
        print(f'💡invalid table name, expect: <schema_name.table_name> got: {table_name}')
        return
    # <schema>.<table>
    schema = table_name.split('.')[0]
    table = table_name.split('.')[1]
    schema_table_pattern = re.compile(rf'''
        \b           # word boundary
        (?:{schema}) # schema, non-capture group
        \.           # .
        (?:{table})  # table, non-capture group
        (?:_today)?  # optional
        \b           # word boundary
    ''', re.VERBOSE)

    sql_files = []
    filters = [
        'backfill',
        'modification_history',
        'etl_optimization_contrast',
    ]
    filters.extend(filter)
    filters = "|".join([f + "/" for f in filters])
    for root, dirs, files in os.walk(directory):
        for file in files:
            if not file.endswith('.sql'):
                continue
            if table not in file:
                filename = os.path.join(root, file)
                if re.search(rf'(?:{filters})', filename):
                    continue
                sql_files.append(filename)

    downstream_sql_files = set()
    for sql_file in sql_files:
        with open(sql_file) as f:
            sql = f.read()

        sql = remove_comments(sql)
        what = schema_table_pattern.search(sql)
        if what:
            downstream_sql_files.add(sql_file)

    for dsf in sorted(downstream_sql_files):
        print(dsf)

    pyperclip.copy('\n'.join(sorted(downstream_sql_files)))


@main.command()
@click.option('-r', '--render/--no-render', default=False)
@click.option('-s', '--dw-latest-partition',
              default=(datetime.utcnow() - timedelta(hours=40)).strftime('%Y/%m/%d/16'))
@click.option('-e', '--dw-eold-partition',
              default=(datetime.utcnow() - timedelta(hours=16)).strftime('%Y/%m/%d/16'))
@click.argument('file_name', required=True, type=click.Path(exists=True))
def etl(
        render,
        dw_latest_partition,
        dw_eold_partition,
        file_name,
):
    work_dir = os.getcwd()
    sql_file_path = f'{work_dir}/{file_name}'

    if render:
        with open(sql_file_path) as f:
            sql = f.read().replace('$dw_latest_partition', dw_latest_partition)
            sql = sql.replace('$dw_eold_partition', dw_eold_partition)
            sql = re.sub(r'drop\s+table\s+(?:if\s+exists\s+)?\w+\.(\w+)', r'drop table if exists \1', sql, flags=re.IGNORECASE)
            sql = re.sub(r'create\s+table\s+\w+\.(\w+)\s+as', r'create temp table \1 as', sql, flags=re.IGNORECASE)

        with open(sql_file_path, 'w') as f:
            f.write(sql + '\n')
    else:
        with open(sql_file_path) as f:
            sql = f.read().replace(f'{dw_latest_partition}', '$dw_latest_partition')
            sql = sql.replace(f'{dw_eold_partition}', '$dw_eold_partition')

        with open(sql_file_path, 'w') as f:
            f.write(sql)


@main.command()
@click.option('--cluster-id', default='bi-sandbox')
@click.option('--start-time', required=True)
@click.option('--end-time', required=True)
def describe_cluster_snapshots(
        cluster_id,
        start_time,
        end_time,
):
    import boto3

    redshift_client = boto3.client('redshift')

    start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') - timedelta(hours=8)
    start_time = start_time.isoformat() + 'Z'

    end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') - timedelta(hours=8)
    end_time = end_time.isoformat() + 'Z'

    res = redshift_client.describe_cluster_snapshots(
        ClusterIdentifier=cluster_id,
        StartTime=start_time,
        EndTime=end_time,
    )

    for snapshot in res.get('Snapshots', []):
        if isinstance(snapshot.get('SnapshotCreateTime'), datetime):
            snapshot['SnapshotCreateTime'] = snapshot.get('SnapshotCreateTime') + timedelta(hours=8)
        print(json.dumps({
            'SnapshotIdentifier': snapshot.get('SnapshotIdentifier'),
            'SnapshotCreateTime': snapshot.get('SnapshotCreateTime'),
            'Status': snapshot.get('Status'),
        }, indent=4, default=str))


@main.command()
@click.option('--prod/--no-prod', default=False)
@click.option('--tablename', required=True, prompt='表名')
@click.option('--tablename-suffix', default='[0-9]{6}')
@click.option('--column-name-list', default='*')
def merge_tablel_partitions(
        prod,
        tablename,
        tablename_suffix,
        column_name_list
):
    dbaddr = os.environ.get('REDSHIFT_SANDBOX')
    if prod:
        dbaddr = os.environ.get('REDSHIFT_PROD')
    with psycopg2.connect(
            f'postgresql://{dbaddr}') as conn:
        with conn.cursor() as cursor:
            sql = textwrap.dedent(f"""
                select table_name
                from svv_all_tables
                where schema_name = 'ods' and regexp_replace(table_name, '{tablename}_{tablename_suffix}', '') = ''
            """)
            print(sql)
            cursor.execute(sql)
            tables = cursor.fetchall()
            # print(tables)
            if tables:
                if column_name_list == '*':
                    all_cols = set()
                    column_name_list = []
                    for table in tables:
                        cols = set()
                        sql = textwrap.dedent(f"""
                            select column_name 
                            from svv_all_columns
                            where schema_name = 'ods' and table_name = '{table[0]}'
                        """)
                        cursor.execute(sql)
                        for column_name in cursor.fetchall():
                            cols.add(column_name[0])
                            all_cols.add(column_name[0])
                        column_name_list.append(cols)

                    for cols in column_name_list:
                        all_cols &= cols
                    column_name_list = ', '.join(all_cols)

                sqls = [f'create or replace view ods.view_{tablename} as']
                partition_pattern = re.compile(rf'({tablename_suffix})')
                for table in sorted(tables):
                    what = partition_pattern.search(table[0])
                    if what and what.group(1) > datetime.strftime(datetime.now(timezone.utc) + timedelta(hours=8), '%Y%m'):
                        continue
                    sqls.append(f'select {column_name_list} from ods.{table[0]} union all')
                sqls[-1] = sqls[-1].replace(' union all', ';')

                pyperclip.copy('\n'.join(sqls))


@main.command()
@click.option('--prod/--no-prod', default=False)
@click.option('--sql', required=True)
def explain(
        prod,
        sql,
):
    dbaddr = os.environ.get('REDSHIFT_SANDBOX')
    if prod:
        dbaddr = os.environ.get('REDSHIFT_PROD')
    with psycopg2.connect(
            f'postgresql://{dbaddr}') as conn:
        with conn.cursor() as cursor:
            sql = f'explain {sql}'
            # print(sql)
            cursor.execute(sql)
            rows = cursor.fetchall()
            if rows:
                text = []
                for row in rows:
                    text.append(row[0])
                plan = '\n'.join(text)
                print(plan)
                pyperclip.copy(plan)


if __name__ == '__main__':
    depends()

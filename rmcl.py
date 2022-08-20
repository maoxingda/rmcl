import os
import re
from pprint import pprint

from datetime import datetime, timedelta

import click
import psycopg2
import pyperclip
from jinja2 import Template


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
@click.option('-s', '--subsidy-snowflake-id', required=True, prompt='餐补雪花ID')
@click.option('-s', '--start-date', required=True, prompt='报表查询开始日期')
@click.option('-e', '--end-date', required=True, prompt='报表查询截止日期')
@click.argument('file_name', required=True, type=click.Path(exists=True))
def s6(
        render,
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
                'subsidy_snowflake_id': subsidy_snowflake_id,
                'date_start': start_date,
                'date_end': end_date,
            })

        with open(sql_file_path, 'w') as f:
            f.write(sql)
    else:
        with open(sql_file_path) as f:
            sql = f.read().replace(subsidy_snowflake_id, '{{subsidy_snowflake_id}}')
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

    sql = multiline_comment_pattern.sub('', sql)
    sql = single_line_pattern.sub('', sql)
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
@click.argument('directory', default='.', type=click.Path(exists=True))
def downstream(
        table_name,
        directory,
):
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

    # <schema>.<table>
    schema = table_name.split('.')[0]
    table = table_name.split('.')[1]
    schema_table_pattern = re.compile(rf'''
        \b           # word boundary
        (?:{schema}) # schema, non-capture group
        \.           # .
        (?:{table})  # table, non-capture group
        \b           # word boundary
    ''', re.VERBOSE)

    sql_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if not file.endswith('.sql'):
                continue
            if table not in file:
                filename = os.path.join(root, file)
                if re.search(r'(?:etl_optimization_contrast|backfill|modification_history|report)', filename):
                    continue
                sql_files.append(filename)

    downstream_sql_files = set()
    for sql_file in sql_files:
        with open(sql_file) as f:
            sql = f.read()

        sql = multiline_comment_pattern.sub('', sql)
        sql = single_line_pattern.sub('', sql)
        what = schema_table_pattern.search(sql)
        if what:
            downstream_sql_files.add(sql_file)

    for dsf in sorted(downstream_sql_files):
        print(dsf)

    pyperclip.copy('\n'.join(sorted(downstream_sql_files)))
    pyperclip.paste()


@main.command()
@click.option('-r', '--render/--no-render', default=False)
@click.option('-s', '--dw-latest-partition',
              default=(datetime.utcnow() - timedelta(hours=40)).strftime('%Y/%m/%d/16'), prompt='增量开始分区')
@click.option('-e', '--dw-eold-partition',
              default=(datetime.utcnow() - timedelta(hours=16)).strftime('%Y/%m/%d/16'), prompt='增量结束分区')
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

        with open(sql_file_path, 'w') as f:
            f.write(sql)
    else:
        with open(sql_file_path) as f:
            sql = f.read().replace(f'{dw_latest_partition}', '$dw_latest_partition')
            sql = sql.replace(f'{dw_eold_partition}', '$dw_eold_partition')

        with open(sql_file_path, 'w') as f:
            f.write(sql)


if __name__ == '__main__':
    depends()

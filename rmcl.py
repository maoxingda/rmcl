import os

import click
from jinja2 import Template


@click.group()
def main():
    pass


@main.command()
@click.option('-r', '--render/--no-render', default=False)
@click.option('-c', '--client-snowflake-id', required=True)
@click.option('-a', '--balance-account-snowflake-id', required=True)
@click.option('-s', '--start-date', required=True)
@click.option('-e', '--end-date', required=True)
@click.argument('file_name', required=True)
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


if __name__ == '__main__':
    main()

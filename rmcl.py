import os

import click
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


if __name__ == '__main__':
    main()

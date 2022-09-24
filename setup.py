from setuptools import setup

setup(
    name='redshift-management-compiler',
    version='0.0.1',
    py_modules=[
        'rmcl',
    ],
    install_requires=[
        'click',
        'jinja2',
        'pyperclip',
        'psycopg2-binary',
        'boto3',
        'sql_metadata',
    ],
    entry_points={
        'console_scripts': [
            'rmcl = rmcl:main',
        ],
    },
)

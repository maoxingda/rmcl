from setuptools import setup

setup(
    name='redshift-management-compiler',
    version='0.0.0',
    py_modules=[
        'rmcl',
    ],
    install_requires=[
        'click',
        'jinja2',
    ],
    entry_points={
        'console_scripts': [
            'rmcl = rmcl:main',
        ],
    },
)

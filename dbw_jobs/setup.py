from setuptools import setup, find_packages

setup(
    name='myfirstwonderfulpackage',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    package_data={
        'validation': ['rules/*.yaml'],
    },
    install_requires=[
        "pyspark==3.1.2",
        "delta-spark==1.0.0",
        "PyYAML>=5.4.1"
    ],
    entry_points={
        'console_scripts': [
            'bronze_to_silver=jobs.bronze_to_silver:cli_main',
            'silver_to_gold=jobs.silver_to_gold:cli_main',
            'smart_city_data_fusion=jobs.smart_city_data_fusion:cli_main',
            'cleaning_env=cleaning.cleaning_env:cli_main'
        ],
    },
)
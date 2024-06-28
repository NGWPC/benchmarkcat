from setuptools import setup, find_packages

setup(
    name='benchmarkcat',
    version='0.1.0',
    packages=find_packages(include=['ingest', 'ingest.*']),
    include_package_data=True,
    install_requires=[
    ],
    entry_points={
        'console_scripts': [
        ],
    },
    setup_requires=['setuptools'],
)


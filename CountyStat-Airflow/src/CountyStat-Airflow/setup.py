from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'Package to run Airflow tasks for a Slack Alert and Hoot Update/Verfication'
LONG_DESCRIPTION = 'Package with functions that can be used on callback, as a python operator, or other utility within and Airflow DAG. Functions include a Slack-Alert for failed tasks and Hoot Status Alert/Update.'

# Setting up
setup(
    # the name must match the folder name 'verysimplemodule'
    name="CountyStat-Airflow",
    version=VERSION,
    author="Daniel Andrus",
    author_email="<daniel.andrus@alleghenycounty.us>",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=['apache-airflow', 'requests'],  # add any additional packages that
    # needs to be installed along with your package. Eg: 'caer'

    keywords=['python', 'first package'],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.10",
        "Operating System :: Microsoft :: Windows",
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Topic :: Communications :: Chat",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ]
)

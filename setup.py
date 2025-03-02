#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="couchbase-cli",
    version="6.0.1",
    description="Couchbase Command Line Tools",
    long_description="This package contains the command line tool set that performs "
                     "the same actions to provide the user the same capabilities as "
                     "the Couchbase Server web interface.",
    author="couchbase",
    packages=find_packages(),
    include_package_data=True,
    url="https://github.com/couchbase/couchbase-cli",
    install_requires=["future", "python-snappy"],
    py_modules=['cb_bin_client', 'cbmgr', 'cluster_manager',
                'pbar', 'pump_bfd', 'pump_csv',
                'pump_gen', 'pump_mc', 'pump_sfd',
                'cb_util', 'couchbaseConstants', 'pump_bfd2',
                'pump_cb', 'pump_dcp', 'pump_json',
                'pump', 'pump_transfer'],
    scripts=['cbbackup',
             'cbbackupwrapper',
             'cblogredaction',
             'cbrecovery',
             'cbrestore',
             'cbrestorewrapper',
             'cbtransfer',
             'cbworkloadgen',
             'couchbase-cli',
             'cb_version.cmake.py'
             ]
)

#!/usr/bin/env -e python

import setuptools
import os, sys
from pip.req import parse_requirements

setuptools.setup(
    name='OCCO-InfoBroker',
    version='0.1.0',
    author='Adam Visegradi',
    author_email='adam.visegradi@sztaki.mta.hu',
    namespace_packages=['occo'],
    packages=['occo.infobroker'],
    scripts=[],
    data_files=[],
    url='http://www.lpds.sztaki.hu/',
    license='LICENSE.txt',
    description='Information Broker',
    long_description=open('README.txt').read(),
    install_requires=['argparse',
                      'PyYAML',
                      'python-dateutil',
                      'redis',
                      'OCCO-Util'],
)

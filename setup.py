    #!/usr/bin/env python

__author__    = "Vivek Balasubramanian"
__email__     = "vivek.balasubramanian@rutgers.edu"
__copyright__ = "Copyright 2017, The RADICAL Project at Rutgers"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip. """

import os
import sys
import subprocess

from setuptools import setup, find_packages, Command

#-----------------------------------------------------------------------------
#
def check_version():
    if  sys.hexversion < 0x02060000 or sys.hexversion >= 0x03000000:
        raise RuntimeError("SETUP ERROR: radical.entk requires Python 2.6 or higher")


check_version()

short_version = 0.6

setup_args = {
    'name'             : 'radical.entk',
    'version'          : short_version,
    'description'      : "Radical Ensemble Toolkit.",
    #'long_description' : (read('README.md') + '\n\n' + read('CHANGES.md')),
    'author'           : 'RADICAL Group at Rutgers University',
    'author_email'     : 'vivek.balasubramanian@rutgers.edu',
    'maintainer'       : "Vivek Balasubramanian",
    'maintainer_email' : 'vivek.balasubramanian@rutgers.edu',
    'url'              : 'https://github.com/radical-cybertools/radical.entk',
    'license'          : 'MIT',
    'keywords'         : "ensemble workflow execution",
    'classifiers'      :  [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix'
    ],

    #'entry_points': {},

    #'dependency_links': ['https://github.com/saga-project/saga-pilot/tarball/master#egg=sagapilot'],

    'namespace_packages': ['radical', 'radical'],
    'packages'          : find_packages('src'),

    'package_dir'       : {'': 'src'},

    'scripts'           : ['bin/entk-version'],
                           

    'package_data'      :  {'': ['*.sh', '*.json', 'VERSION', 'SDIST']},

    'install_requires'  :  ['radical.utils', 'setuptools>=1', 'pika'],
    #'test_suite'        : 'radical.entk.tests',

    'zip_safe'          : False,
    # This copies the contents of the examples/ dir under
    # sys.prefix/share/radical.pilot.
    # It needs the MANIFEST.in entries to work.
}

setup (**setup_args)

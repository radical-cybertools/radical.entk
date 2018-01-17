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


# borrowed from the MoinMoin-wiki installer
#
def makeDataFiles(prefix, dir):
    """ Create distutils data_files structure from dir
    distutil will copy all file rooted under dir into prefix, excluding
    dir itself, just like 'ditto src dst' works, and unlike 'cp -r src
    dst, which copy src into dst'.
    Typical usage:
        # install the contents of 'wiki' under sys.prefix+'share/moin'
        data_files = makeDataFiles('share/moin', 'wiki')
    For this directory structure:
        root
            file1
            file2
            dir
                file
                subdir
                    file
    makeDataFiles('prefix', 'root')  will create this distutil data_files structure:
        [('prefix', ['file1', 'file2']),
         ('prefix/dir', ['file']),
         ('prefix/dir/subdir', ['file'])]
    """
    # Strip 'dir/' from of path before joining with prefix
    dir = dir.rstrip('/')
    strip = len(dir) + 1
    found = []
    os.path.walk(dir, visit, (prefix, strip, found))
    #print found[0]
    return found[0]

def visit((prefix, strip, found), dirname, names):
    """ Visit directory, create distutil tuple
    Add distutil tuple for each directory using this format:
        (destination, [dirname/file1, dirname/file2, ...])
    distutil will copy later file1, file2, ... info destination.
    """
    files = []
    # Iterate over a copy of names, modify names
    for name in names[:]:
        path = os.path.join(dirname, name)
        # Ignore directories -  we will visit later
        if os.path.isdir(path):
            # Remove directories we don't want to visit later
            if isbad(name):
                names.remove(name)
            continue
        elif isgood(name):
            files.append(path)
    destination = os.path.join(prefix, dirname[strip:])
    found.append((destination, files))

def isbad(name):
    """ Whether name should not be installed """
    return (name.startswith('.') or
            name.startswith('#') or
            name.endswith('.pickle') or
            name == 'CVS')

def isgood(name):
    """ Whether name should be installed """
    if not isbad(name):
        if name.endswith('.py') or name.endswith('.json'):
            return True
    return False

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

    'install_requires'  :  ['radical.utils==namespaces','radical.pilot', 'pika'],

    'zip_safe'          : False,
    
    'data_files'        : [
                                    makeDataFiles('share/radical.entk/user_guide/scripts/', 'examples/user_guide'),
                                    makeDataFiles('share/radical.entk/simple_examples/scripts/', 'examples/simple_examples')
                            ],

    'dependency_links': ['git+https://github.com/radical-cybertools/radical.utils.git@feature/id_namespaces#egg=radical.utils-namespaces']

}

setup (**setup_args)

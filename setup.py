#!/usr/bin/env python

__author__    = "Vivek Balasubramanian"
__email__     = "vivek.balasubramanian@rutgers.edu"
__copyright__ = "Copyright 2014, The RADICAL Project at Rutgers"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip. """

import os
import sys
import subprocess

from setuptools import setup, find_packages, Command

#-----------------------------------------------------------------------------
#
def read(*rnames):
	return open(os.path.join(os.path.dirname(__file__), *rnames)).read()

#-----------------------------------------------------------------------------
#
def check_version():
	if  sys.hexversion < 0x02060000 or sys.hexversion >= 0x03000000:
		raise RuntimeError("SETUP ERROR: radical.ensemblemd requires Python 2.6 or higher")

#-----------------------------------------------------------------------------
#
def get_version():
	short_version = None  # 0.4.0
	long_version  = None  # 0.4.0-9-g0684b06

	try:
		import subprocess as sp
		import re

		srcroot       = os.path.dirname (os.path.abspath(__file__))
		VERSION_MATCH = re.compile(r'(([\d\.]+)\D.*)')

		# attempt to get version information from git
		p   = sp.Popen ('cd %s && git describe --tags --always' % srcroot,
						stdout=sp.PIPE, stderr=sp.STDOUT, shell=True)
		out = p.communicate()[0]

		if  p.returncode != 0 or not out:

			# the git check failed -- its likely that we are called from
			# a tarball, so use ./VERSION instead
			out=open("%s/VERSION" % ".", 'r').read().strip()

		# from the full string, extract short and long versions
		v = VERSION_MATCH.search (out)
		if v:
			long_version  = v.groups()[0]
			short_version = v.groups()[1]

		# sanity check if we got *something*
		if not short_version or not long_version:
			raise RuntimeError("SETUP ERROR: Cannot determine version from git or ./VERSION\n")

		short_version = open('VERSION', 'r').read().strip()

		# make sure the version files exist for the runtime version inspection
		#open ('%s/VERSION' % srcroot, 'w').write (long_version+"\n")
		open ('%s/src/radical/ensemblemd/VERSION' % srcroot, 'w').write (long_version+"\n")


	except Exception as e :
		raise RuntimeError("SETUP ERROR: Could not extract/set version: %s" % e)

	return short_version, long_version


# ------------------------------------------------------------------------------
#
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



#-----------------------------------------------------------------------------
#
srcroot = os.path.dirname(os.path.realpath(__file__))
check_version()
short_version, long_version = get_version()

setup_args = {
	'name'             : 'radical.ensemblemd',
	'version'          : short_version,
	'description'      : "Radical Ensemble Molecular Dynamics (MD) Toolkit.",
	'long_description' : (read('README.md') + '\n\n' + read('CHANGES.md')),
	'author'           : 'RADICAL Group at Rutgers University',
	'author_email'     : 'vivek.balasubramanian@rutgers.edu',
	'maintainer'       : "Vivek Balasubramanian",
	'maintainer_email' : 'vivek.balasubramanian@rutgers.edu',
	'url'              : 'https://github.com/radical-cybertools/radical.ensemblemd',
	'license'          : 'MIT',
	'keywords'         : "molecular dynamics toolkit python",
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

	#'entry_points': {
	#    'console_scripts':
	#        ['htbac-fecalc = radical.ensemblemd.htbac.bin.fecalc:main',
	#         'htbac-sim    = radical.ensemblemd.htbac.bin.sim:main']
	#},

	#'dependency_links': ['https://github.com/saga-project/saga-pilot/tarball/master#egg=sagapilot'],

	'namespace_packages': ['radical', 'radical'],
	'packages'          : find_packages('src'),

	'package_dir'       : {'': 'src'},

	'scripts'           : ['bin/ensemblemd-version'],
						   

	'package_data'      :  {'': ['*.sh', '*.json', 'VERSION', 'VERSION.git']},

	'install_requires'  :  ['radical.utils','radical.pilot', 'setuptools>=1'],
	#'test_suite'        : 'radical.ensemblemd.tests',

	'zip_safe'          : False,
	# This copies the contents of the examples/ dir under
	# sys.prefix/share/radical.pilot.
	# It needs the MANIFEST.in entries to work.
	'data_files'        : [
									makeDataFiles('share/radical.ensemblemd/examples/', 'examples'),
									makeDataFiles('share/radical.ensemblemd/examples/amber_input', 'examples/amber_input'),
									makeDataFiles('share/radical.ensemblemd/user_guide/scripts', 'doc/user_guide/scripts')
						  ],
}

setup (**setup_args)

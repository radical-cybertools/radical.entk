import os

srcroot = os.path.dirname(os.path.realpath(__file__))
version = open('{0}/VERSION'.format(srcroot), 'r').readline().rstrip()
__version__ = version

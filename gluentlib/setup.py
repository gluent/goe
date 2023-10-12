"""
    Gluentlib: A collection of standardized python tools and abstractions
               used in Gluent projects.
"""

from setuptools import setup, find_packages
import sys

def version():
    with open('../version') as f:
        return f.read()

def python_version():
    return '%s.%s' % (sys.version_info.major, sys.version_info.minor)

setup(name='gluentlib',
      version=version(),
      description='Gluent Python libraries and tools',
      long_description='Python libraries and tools used by Gluent Data Platform',
      url='gluent.com',
      author='Gluent Inc.',
      author_email='feedback@gluent.com',
      license='Proprietary',
      platforms=['Red Hat Enterprise Linux, CentOS, Oracle Linux, 64-bit, versions 6 and 7'],
      classifiers =[
          'Development Status :: 5 - Production/Stable',
          'License :: Other/Proprietary License',
          'Programming Language :: Python :: %s' % python_version()
      ],
      package_dir={'': 'build/src'},
      packages=find_packages(where="build/src"),
      package_data={"gluentlib": ["listener/web/public/docs/*.*"]},
      install_requires=[
          'setuptools>=18.2',
          'termcolor',
          'boto3',
          'hdfs',
          'impyla>=0.12',
          'pytz',
          'pyyaml',
      ]
)

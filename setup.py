"""
esmarc is a python3 tool to read line-delimited MARC21 JSON from an elasticSearch index, perform a mapping and writes the output in a directory with a file for each mapping type.
"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='esmarc',
      version='1.2',
      description='esmarc is a python3 tool to read line-delimited MARC21 JSON from an elasticSearch index, perform a mapping and writes the output in a directory with a file for each mapping type.',
      url='https://github.com/slub/esmarc',
      author='Bernhard Hering',
      author_email='bernhard.hering@slub-dresden.de',
      license="Apache 2.0",
      packages=['esmarc','swb_fix'],
      package_dir={'esmarc': 'esmarc',
                   'swb_fix': 'esmarc'},
      install_requires=[
          'argparse>=1.4.0',
          'elasticsearch>=5.0.0'
      ],
      python_requires=">=3.6.*",
      entry_points={
          "console_scripts": ["esmarc=esmarc.esmarc:main"]
          }
      )

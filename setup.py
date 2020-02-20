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
      packages=[
          'esmarc',
          'swb_fix',
          'wikidata',
          'geonames',
          'gnd_sachgruppen',
          'entityfacts',
          'enrichment'
      ],
      package_dir={
          'esmarc': 'esmarc',
          'swb_fix': 'esmarc',
          'enrichment': 'enrichment',
          'wikidata': 'enrichment',
          'entityfacts': 'enrichment',
          'geonames': 'enrichment',
          'gnd_sachgruppen': 'enrichment'
      },
      install_requires=[
          'argparse>=1.4.0',
          'elasticsearch>=5.0.0',
          'rdflib>=4.2.2'
      ],
      python_requires=">=3.5.*",
      entry_points={
          "console_scripts": [
              "esmarc=esmarc.esmarc:main",
              "wikidata.py=enrichment.wikidata:run",
              "wikipedia.py=enrichment.wikipedia:run",
              "entityfacts.py=enrichment.entityfacts:run",
              "geonames.py=enrichment.geonames:run",
              "gnd_sachgruppen.py=enrichment.gnd_sachgruppen:run",
              ]
          }
      )

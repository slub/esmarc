"""
esmarc is a python3 tool to read line-delimited MARC21 JSON from an elasticSearch index, perform a mapping and writes the output in a directory with a file for each mapping type.
"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='esmarc',
      version='1.3',
      description='esmarc is a python3 tool to read line-delimited MARC21 JSON from an elasticSearch index, perform a mapping and writes the output in a directory with a file for each mapping type.',
      url='https://github.com/slub/esmarc',
      author='SLUB LOD Team',
      author_email='lod.team@slub-dresden.de',
      license="Apache 2.0",
      packages=[
          'esmarc',
          'swb_fix',
          'wikidata',
          'wikipedia',
          'wikipedia_categories',
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
          'wikipedia': 'enrichment',
          'wikipedia_categories': 'enrichment',
          'entityfacts': 'enrichment',
          'geonames': 'enrichment',
          'gnd_sachgruppen': 'enrichment'
      },
      install_requires=[
          'argparse>=1.4.0',
          'elasticsearch>=8.0.0',
          'rdflib>=4.2.2',
          'dateparser',
          'urllib"
      ],
      python_requires=">=3.5.*",
      entry_points={
          "console_scripts": [
              "esmarc=esmarc.esmarc:cli",
              "wikidata.py=enrichment.wikidata:run",
              "wikipedia.py=enrichment.wikipedia:run",
              "wikipedia_categories.py=enrichment.wikipedia_categories:run",
              "entityfacts.py=enrichment.entityfacts:run",
              "geonames.py=enrichment.geonames:run",
              "gnd_sachgruppen.py=enrichment.gnd_sachgruppen:run",
              ]
          }
      )

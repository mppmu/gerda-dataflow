from setuptools import setup, find_packages

setup(
    name = 'gerda-dataflow',
    version = '0.1',
    description = 'GERDA Data Flow Framework',
    url = 'http://github.com/mppmu/gerda-dataflow',
    author = 'Oliver Schulz',
    author_email = 'oschulz@mpp.mpg.de',
    license = 'Apache License 2.0',
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    scripts=['src/gerda/dataflow/gerda-raw-conv.sh'],
    install_requires = [
        'arrow',
        'enum34',
        'luigi',
        'radical.pilot',
        'saga-python',
        'subprocess32'
    ],
    zip_safe = False
)

from setuptools import setup, find_packages

setup(
    name = 'gerda-dataflow',
    version = '0.1',
    description = 'GERDA Data Stream Control',
    url = 'http://github.com/mppmu/gerda-dataflow',
    author = 'Oliver Schulz',
    author_email = 'oschulz@mpp.mpg.de',
    license = 'LGPL',
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    install_requires = [
        'saga-python',
    ],
    zip_safe = False
)

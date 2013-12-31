import os

from distribute_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
readme = open(os.path.join(here, 'README.md')).read()
requires = open(os.path.join(here, 'requirements.txt')).read()
requires = map(lambda r: r.strip(), requires.splitlines())
test_requires = open(os.path.join(here, 'test_requirements.txt')).read()
test_requires = map(lambda r: r.strip(), test_requires.splitlines())

setup(
    name='invoice_feeder',
    version='0.0.1',
    author='Balanced Payment',
    author_email='support@balancedpayments.com',
    url='https://github.com/balanced/invoice_feeder',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    tests_require=test_requires,
)

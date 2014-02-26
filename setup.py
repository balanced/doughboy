import os

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
#readme = open(os.path.join(here, 'README.md')).read()
requires = open(os.path.join(here, 'requirements.txt')).read()
requires = map(lambda r: r.strip(), requires.splitlines())
requires = filter(lambda r: not r.startswith('-e '), requires)  # FIXME:
test_requires = open(os.path.join(here, 'test-requirements.txt')).read()
test_requires = map(lambda r: r.strip(), test_requires.splitlines())

setup(
    name='doughboy',
    version='0.0.0',
    author='Balanced Payment',
    author_email='support@balancedpayments.com',
    url='https://github.com/balanced/doughboy',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    tests_require=test_requires,
    entry_points="""\
    [console_scripts]
    doughboy = doughboy.process_event:main
    """,
)

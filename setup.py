from setuptools import setup, find_packages
import pydht

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='pydht',
    version=pydht.__version__,
    description='Python DHT Implementation',
    long_description=readme,
    author='Isaac Zafuta',
    author_email='isaac@zafuta.com',
    url='https://github.com/isaaczafuta/pydht',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

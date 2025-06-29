# from codecs import open
from os import path

from setuptools import find_packages, setup  # pylint: disable=import-error

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='parproc',
    version='0.3.2',
    description='Process Parallelization',
    long_description=long_description,
    long_description_content_type='text/markdown',
    # url='',
    author='Oivind Loe',
    author_email='oivind.loe@gmail.com',
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[  # Optional
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='development scripting',
    packages=find_packages(exclude=['tests']),  # Required
    install_requires=[],
    python_requires='>=3.8',
)

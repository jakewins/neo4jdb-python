import os
import tarfile
from subprocess import call
from paver.easy import *
from paver.setuputils import setup, find_packages

try:
    from urllib.request import urlretrieve
except ImportError:
    from urllib import urlretrieve

setup(
    name='neo4jdb',
    version='0.0.8',
    author='Jacob Hansson',
    author_email='jakewins@gmail.com',
    packages=find_packages(),
    py_modules=['setup'],
    include_package_data=True,
    install_requires=[],
    url='https://github.com/jakewins/neo4jdb-python',
    description='DB API 2.0 driver for the Neo4j graph database.',
    long_description=open('README.rst').read(),
    classifiers=[
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
    ],
)

BUILD_DIR = 'build'
NEO4J_VERSION = '2.1.3'


@task
@needs('generate_setup', 'minilib', 'setuptools.command.sdist')
def sdist():
    """Overrides sdist to make sure that our setup.py is generated."""
    pass


@task
def start_server():
    if not os.path.exists(BUILD_DIR):
        os.makedirs(BUILD_DIR)

    if not path(BUILD_DIR + '/neo4j-server.tar.gz').access(os.R_OK):
        print("Downloading Neo4j Server")
        urlretrieve("http://download.neo4j.org/artifact?edition=community&version=%s&distribution=tarball" % NEO4J_VERSION, BUILD_DIR + "/neo4j-server.tar.gz")

    if not path(BUILD_DIR + '/neo4j-server').access(os.R_OK):
        print("Unzipping Neo4j Server..")
        tar = tarfile.open(BUILD_DIR + "/neo4j-server.tar.gz")
        tar.extractall(BUILD_DIR)
        tar.close()
        os.rename(BUILD_DIR + "/neo4j-community-%s" % NEO4J_VERSION, BUILD_DIR + "/neo4j-server")

    call([BUILD_DIR + "/neo4j-server/bin/neo4j", "start"])


@task
def stop_server():
    if path(BUILD_DIR + '/neo4j-server').access(os.R_OK):
        call([BUILD_DIR + "/neo4j-server/bin/neo4j", "stop"])

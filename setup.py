# -*- encoding: utf8 -*-
import glob
import io
import sys
from os.path import basename, dirname, join, splitext

from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand


def read(*names, **kwargs):
    return io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ).read()


class Tox(TestCommand):
    user_options = [('tox-args=', 'a', "Arguments to pass to tox")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.tox_args = None

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import tox
        import shlex
        errno = tox.cmdline(args=shlex.split(self.tox_args))
        sys.exit(errno)

setup(
    name="crashdb",
    version="3.0.3",
    license="MIT",
    description="ETL Crash Data",
    long_description="",
    author="Steve Gourley",
    author_email="sgourley@utah.gov",
    url="https://github.com/agrc/crash-db",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(i))[0] for i in glob.glob("src/*.py")],
    package_data={"crashdb": ['connections/*.sde', 'data/sql/*.sql', 'pickup/*.md']},
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Topic :: Utilities",
    ],
    keywords=[
        # eg: "keyword1", "keyword2", "keyword3",
    ],
    install_requires=[
        "docopt==0.6.2",
        "google-api-python-client==1.7.7",
        "google-auth-httplib2==0.0.3",
        "google-auth==1.31.0",
        "google-cloud-storage==1.38.0",
        "pyodbc==4.0.23",
        "pyproj==2.2.2",
        "pysftp==0.2.9",
        "python-dateutil==2.7.5",
    ],
    extras_require={
        # eg: 'rst': ["docutils>=0.11"],
    },
    entry_points={
        "console_scripts": [
            "crashdb = crashdb.__main__:main"
        ]
    },
    cmdclass={
        'test': Tox
    },
    tests_require=[
        'tox',
        'nose==1.3.4',
        'coverage==3.7.1'
    ],
)

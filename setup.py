from __future__ import print_function
__version__ = '1.0.13'
__author__  = 'BiSkilled'
__codename__= 'Why not?'

import os.path
import sys
import warnings

try:
    from setuptools import setup, find_packages
    from setuptools.command.build_py import build_py as BuildPy
    from setuptools.command.install_lib import install_lib as InstallLib
    from setuptools.command.install_scripts import install_scripts as InstallScripts
except ImportError:
    print("dingDong now needs setuptools in order to build. Install it using"
          " your package manager (usually python-setuptools) or via pip (pip"
          " install setuptools).", file=sys.stderr)
    sys.exit(1)


sys.path.insert(0, os.path.abspath('lib'))
#print(sys.path)
#from release import  __version__, __author__

def read_file(file_name):
    """Read file and return its contents."""
    with open(file_name, 'r') as f:
        print (file_name)
        return f.read()

def read_requirements(file_name):
    """Read requirements file as a list."""
    reqs = read_file(file_name).splitlines()
    if not reqs:
        raise RuntimeError(
            "Unable to read requirements from the %s file"
            "That indicates this copy of the source code is incomplete."
            % file_name
        )
    return reqs

def get_dynamic_setup_params():
    """Add dynamically calculated setup params to static ones."""
    return {
        # Retrieve the long description from the README
        'long_description': read_file('README.rst'),
        'install_requires': read_requirements('requirements.txt')
    }

# packages=['', 'glob', 'mapp', 'loader', 'connections'],
#download_url = 'https://github.com/biskilled/popEye-Etl/archive/1.1.0.tar.gz',  # I explain this later on
#install_requires = ['pyodbc', 'pymysql', 'vertica_python', 'cx_Oracle', 'pymongo', 'sqlparse', 'clr', 'pandas'],

static_setup_params = dict(
    name        = 'dingDong',  # How you named your package folder (MyLib)
    version     = __version__,
    description = 'Data modeling managing and transforming data',  # Give a short description about your library
    author      = __author__,
    author_email= 'Tal@BiSkilled.com',  # Type in your E-Mail
    keywords    = ['ETL', 'Data modeling', 'Python', 'Integration', 'Mapping'],   # Keywords that define your package best
    url         = 'https://github.com/biskilled/dingDong',  # Provide either the link to your github or to your website

    project_urls={
        'Bug Tracker'    : 'https://github.com/ansible/ansible/issues',
        'CI: Shippable'  : 'https://app.shippable.com/github/biskilled/dingDong',
        'Code of Conduct': '',
        'Documentation'  : 'https://readthedocs.org/projects/dingDong/',
        'Mailing lists'  : '',
        'Source Code'    : 'https://github.com/biskilled/dingDong',
    },
    license='GPLv3+',
    python_requires= '>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*',
    package_dir    = {'': 'lib'},
    packages       = find_packages('lib'),
    package_data   ={
        '': [ 'dll/*.*' ],
    },

    classifiers=[
        'Development Status :: 4 - Beta',       # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Systems Administration',
        'Topic :: Utilities',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    scripts=[],
    data_files=[],
    # Installing as zip files would break due to references to __file__
    zip_safe=False,
    install_requires=read_requirements('requirements.txt')
)


def main():
    """Invoke installation process using setuptools."""
    setup_params = dict(static_setup_params, **get_dynamic_setup_params())
    ignore_warning_regex = (
        r"Unknown distribution option: '(project_urls|python_requires)'"
    )
    warnings.filterwarnings(
        'ignore',
        message=ignore_warning_regex,
        category=UserWarning,
        module='distutils.dist',
    )
    setup(**setup_params)
    warnings.resetwarnings()

if __name__ == '__main__':
    main()
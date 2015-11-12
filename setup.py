import os
import sys
from setuptools import setup, find_packages

try:
    import pulsar       # noqa
    import botocore     # noqa
except ImportError:
    os.environ['pulsar_cloud_setup'] = 'yes'

package_name = 'pulsar-cloud'
mod = __import__('cloud')


def read(fname):
    with open(fname) as f:
        return f.read()


def requirements():
    req = read('requirements.txt').replace('\r', '').split('\n')
    return [r for r in req if r]


def run(argv=None):
    if argv:
        sys.argv = list(argv)
    setup(name=package_name,
          zip_safe=False,
          version=mod.__version__,
          author=mod.__author__,
          author_email=mod.__contact__,
          url=mod.__homepage__,
          license='BSD',
          description=mod.__doc__,
          install_requires=requirements(),
          packages=find_packages(),
          classifiers=['Development Status :: 3 - Alpha',
                       'Environment :: Plugins',
                       'Intended Audience :: Developers',
                       'License :: OSI Approved :: BSD License',
                       'Operating System :: OS Independent',
                       'Programming Language :: Python',
                       'Programming Language :: Python :: 3.4',
                       'Programming Language :: Python :: 3.5',
                       'Topic :: Utilities'])


if __name__ == '__main__':
    run()

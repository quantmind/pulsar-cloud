import sys
import os

from setuptools import setup, find_packages

import cloud


def read(name):
    filename = os.path.join(os.path.dirname(__file__), name)
    with open(filename) as fp:
        return fp.read()


def requirements(name):
    install_requires = []
    dependency_links = []

    for line in read(name).split('\n'):
        if line.startswith('-e '):
            link = line[3:].strip()
            if link == '.':
                continue
            dependency_links.append(link)
            line = link.split('=')[1]
        line = line.strip()
        if line:
            install_requires.append(line)

    return install_requires, dependency_links


meta = dict(
    version=cloud.__version__,
    description=cloud.__doc__,
    name='pulsar-cloud',
    author="Luca Sbardella",
    author_email="luca@quantmind.com",
    url="https://github.com/quantmind/pulsar-cloud",
    zip_safe=False,
    license='BSD',
    long_description=read('README.rst'),
    setup_requires=['pulsar', 'wheel'],
    install_requires=requirements('requirements.txt')[0],
    packages=find_packages(include=['cloud', 'cloud.*']),
    scripts=['bin/s3upload.py'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Utilities']
)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'agile':
        from agile.app import AgileManager
        AgileManager(description='Release manager for pulsar-cloud',
                     argv=sys.argv[2:]).start()
    else:
        setup(**meta)

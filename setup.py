import os
import sys

from setuptools import setup, find_packages

sys.path.insert(0, os.path.dirname(__file__))
config = __import__('config')


def run():

    meta = dict(
        name='pulsar-cloud',
        author="Luca Sbardella",
        author_email="luca@quantmind.com",
        url="https://github.com/quantmind/pulsar-cloud",
        zip_safe=False,
        license='BSD',
        long_description=config.read('README.rst'),
        setup_requires=['pulsar'],
        install_requires=config.requirements('requirements.txt')[0],
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

    setup(**config.setup(meta, 'cloud'))


if __name__ == '__main__':
    run()

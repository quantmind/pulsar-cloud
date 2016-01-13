import os

from agile.release import ReleaseManager


version_file = os.path.join(os.path.dirname(__file__),
                            'cloud', '__init__.py')


if __name__ == '__main__':
    ReleaseManager(config='release.py').start()

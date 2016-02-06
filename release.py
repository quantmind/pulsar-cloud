from agile import AgileManager


app_module = 'cloud'
note_file = 'docs/notes.md'
docs_bucket = 'quantmind-docs'


if __name__ == '__main__':
    AgileManager(config='release.py').start()

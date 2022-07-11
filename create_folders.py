import os


def create_folders(parent_dir):
    directories = ['storage', 'spam', 'temp','DATA']
    for directory in directories:
        path = os.path.join(parent_dir, directory)
        os.mkdir(path)
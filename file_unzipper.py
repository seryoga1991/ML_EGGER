import zipfile,fnmatch,os


def file_unzipper(unzip_from_directory,move_to_directory):
    pattern = '*.zip'
    for root, dirs, files in os.walk(unzip_from_directory):
        for filename in fnmatch.filter(files, pattern):
            zipfile.ZipFile(os.path.join(root, filename)).extractall(move_to_directory)
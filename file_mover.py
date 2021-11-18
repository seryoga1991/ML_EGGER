import os
import shutil
import config as cfg
import glob
docs = str


def move_files(MOVE_FROM_DIRECTORY, MOVE_TO_DIRECTORY):
    datatypes_to_move = [".pdf", ".PDF", ".xlsx",
                         ".doc", ".DOC", ".XLSX", ".xls", ".XLS", ".Pdf", ".Doc", ".Xls", ".Xlsx", ".Docx", ".DOCX", ".docx"]
    files = [f for f in os.listdir(MOVE_FROM_DIRECTORY) if os.path.isfile(
        os.path.join(MOVE_FROM_DIRECTORY, f))]

    for file in files:
        if list(filter(file.endswith, datatypes_to_move)) != []:
            shutil.move(os.path.join(MOVE_FROM_DIRECTORY, file.title()),
                        os.path.join(MOVE_TO_DIRECTORY, file.title()))


def move_spam(data_to_move, move_from_dir=cfg.path_to_wordlists, move_to_dir=cfg.spam_path):
    for doc in data_to_move:
        doc_types = doc + '.*'
        files_to_move = glob.glob(os.path.join(move_from_dir, doc_types))
        for file in files_to_move:
            try:
                shutil.move(file, move_to_dir)
            except:
                # file does not exist and cant be moved
                print(f'Datei {file} existiert nicht')

import pdfkit
import os
import sys

def convert_htm_to_pdf(PATH_TO_FILES,SAVE_PATH):
    save_path = SAVE_PATH
    path_to_files = PATH_TO_FILES
    files = [f for f in os.listdir(path_to_files) if os.path.isfile(f)]
    for file in files:
        if file.endswith(".htm") | file.endswith(".html"):
            try:
                out_pdf_name = os.path.join(save_path ,file.title().rsplit('.', 1)[0] + ".pdf")
                pdfkit.from_file(file.title(), out_pdf_name)
            except Error:
                pass # gegebene Fehlerbehandlung

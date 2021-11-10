import win32com.client
import os
import pathlib
import email
import sys
import shutil


def save_mail_attachment(PATH_TO_FILES, SAVE_PATH):
    type_message = ".MSG"
    type_email = ".EML"
    type_pdf = ".PDF"
    mime_parser_path = os.path.dirname(os.path.abspath(__file__))
    path_to_files = PATH_TO_FILES
    save_path = SAVE_PATH
    files = [f for f in os.listdir(path_to_files) if os.path.isfile(
        os.path.join(path_to_files, f))]
    outlook = win32com.client.Dispatch(
        "Outlook.Application").GetNamespace("MAPI")
    for file in files:
        filepath = os.path.join(path_to_files, file.title())
        if file.endswith(type_message):
            msg = outlook.OpenSharedItem(filepath)
            attachments = msg.Attachments
            count = 1
            for attachment in attachments:
                try:
                    # Saves the file with the attachment name
                    file_name = file.title()[8:18] + '_' + \
                        str(count) + '.' + attachment.FileName.split('.')[-1]
                    attachment.SaveAsFile(os.path.join(
                        save_path, file_name))
                    count += 1
                except Exception as detail:
                    pass
        elif file.endswith(type_email):
            msg = email.message_from_file(open(filepath))
            attachments = msg.get_payload()
            count = 1
            for attachment in attachments:
                try:
                    file_name = file.title()[
                        8:18] + '_' + str(count) + '.' + str(attachment.get_filename()).split('.')[-1]
                    if file_name.split('.')[-1].lower() != 'pdf':
                        continue
                    f = open(os.path.join(save_path, file_name), 'wb').write(
                        attachment.get_payload(decode=True))
                    f.close()
                    count += 1
                except Exception as detail:

                    pass
        elif file.endswith(type_pdf):  # PDF einfach nur verschieben
            src_file = os.path.join(path_to_files, file.title())
            shutil.copy(src_file, save_path)
            target_file = os.path.join(save_path, file.title())
            new_target_file_name = os.path.join(save_path, file.title()[8:])
            os.rename(target_file, new_target_file_name)



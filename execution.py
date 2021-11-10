import data_loader as dtl 
import file_mover as fmv
import htm_to_pdf as cnv
import mime_parser as prs 
import os,sys

# Pfade müssen dynamisch geholt werden...hier erstmal Platzhalter
path_to_files = 'test'
path_to_sap_files = 'test'
destination_path = 'test'

#Parse erstmal die Mails aus SAP nach Anhang und sichere diesen im Ordner "destination_path"
prs.save_mail_attachment(path_to_files,destination_path)

#Fertig geparst was geht. Jetzt kann man noch die HTM Daten zu PDFs konvertieren (wird für den TBCS gebraucht)
cnv.convert_htm_to_pdf(path_to_files,destination_path)

#Konvertierung ist fertig nun kann der tbcs angestoßen werden
# Funktion fehlt noch

#Falls die Daten bewegt werden müssten dann wird nun der mover gestartet
fmv.move_files(path_to_files,destination_path)

#tbcs ist fertig nun kann man die Daten laden und anfangen zu prozessieren





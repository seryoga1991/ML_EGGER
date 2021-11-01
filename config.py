import os
import multiprocessing
# Allgemeine Werte
cpu_count = multiprocessing.cpu_count()
cores_to_use = int(cpu_count)
path_to_wordlists = "C:\\Users\\agoyy\\OneDrive\\Desktop\\Egger\\storage"
path_to_sap_data = "C:\\Users\\agoyy\\OneDrive\\Desktop\\Egger\\storage\\Zusatzdaten gesamt"
tangro_preproc = {"tangro_om": [
    ["ORDER_HEADERS_OUT", "PURCH_NO"],
    ["BAPICUVALM", "VALUE"],
    ["ORDER_ITEMS_OUT", "SHORT_TEXT"],
    ["ORDER_PARTNERS_OUT", "UNLOAD_PT"]]
}
name_appendix = ''
temporary_save_dir = 'C:\\Users\\agoyy\\OneDrive\\Desktop\\temp\\'
save_dir = 'C:\\Users\\agoyy\\OneDrive\\Desktop\\DATA\\'
# tangro OM spezifische Konfiguration
tangro_om = 'tangro_om'
sd_headers = 'ORDER_HEADERS_OUT.csv'
sd_items = 'ORDER_ITEMS_OUT.csv'
sd_partners = 'ORDER_PARTNERS_OUT.csv'
sd_cuval = 'BAPICUVALM.csv'
sd_cuins = 'BAPICUINSM.csv'
sd_cuprt = 'BAPICUPRTM.csv'
sd_cucfg = 'BAPICUCFGM.csv'
sd_schedules = 'ORDER_SCHEDULES_OUT.csv'
sd_sap_data = [sd_headers, sd_items, sd_partners,
               sd_schedules, sd_cuval, sd_cucfg, sd_cuprt, sd_cuins]
path_to_headers = os.path.join(path_to_sap_data, sd_headers)
path_to_items = os.path.join(path_to_sap_data, sd_items)
path_to_schedules = os.path.join(path_to_sap_data, sd_schedules)
path_to_cuvalm = os.path.join(path_to_sap_data, sd_cuval)
path_to_cuinsm = os.path.join(path_to_sap_data, sd_cuins)
path_to_cuprtm = os.path.join(path_to_sap_data, sd_cuprt)
path_to_cucfgm = os.path.join(path_to_sap_data, sd_cucfg)
path_to_partners = os.path.join(path_to_sap_data, sd_partners)
sd_key_val_pair = {"headers": "DOC_NUMBER", "items": "DOC_NUMBER",
                   "schedules": "DOC_NUMBER", "partners": "SD_DOC",
                   "cuval": "SD_DOC", "cucfg": "SD_DOC",
                   "cuins": "SD_DOC", "cuprt": "SD_DOC"}

# Weitere Module später hier :

tangro_im = ["tangro_im"]
tangro_oc = ["tangro_oc"]
tangro_dm = ["tangro_dm"]


def printProgressBar(percentage, prefix='', suffix='', length=100):  # für Single Threading
    decimals = 1
    fill = '█'
    printEnd = "\r"
    percent = ("{0:." + str(decimals) + "f}").format(100 * percentage)
    filledLength = int(length * percentage)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    # Print New Line on Complete
    if percentage == 100.0:
        print()

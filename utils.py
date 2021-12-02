import glob
import os
import config as cfg


def clean_tmp_data():
    all_files = glob.glob(os.path.join(
        cfg.temporary_save_dir, "*.csv"))
    for f in all_files:
        os.remove(f)


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

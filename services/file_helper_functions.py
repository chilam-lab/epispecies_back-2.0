from services.clean_csv import clean_csv_in_chunks
from os import listdir, remove, rename
from os.path import join, exists

"""
Class for functions that need to use the file system (search, create, iterate, etc.).
"""

def get_csv_in_directory_to_clean() -> bool:
    """
    Get all csv files in the csv directory of the project and 
    apply the cleaning process on each.

    Returns:

    --- True if at least a csv file was cleaned. \n
    --- False if directory is empty or no csv files were found.
    """
    cleaned_a_csv = False
    if len(listdir("db/")) == 0:
        return cleaned_a_csv
    csvs_dir = "db"
    dir_clean_csv = "cleanedCSV/"
    db_suffix = "_db.csv"
    pop_suffix = "_pop.csv"
    mps_suffix = "_mps.csv"
    for file in listdir("db/"):
        if file.endswith(".csv"):
            csv_file_dir = join(csvs_dir, file)
            name_of_cleaned_file = join(dir_clean_csv, file)
            if file.startswith("Pob"):
                name_of_cleaned_file = name_of_cleaned_file[:-4] + pop_suffix
            elif file.startswith("CVE_Metro"):
                name_of_cleaned_file = name_of_cleaned_file[:-4] + mps_suffix
            else:
                name_of_cleaned_file = name_of_cleaned_file[:-4] + db_suffix
            if not exists(name_of_cleaned_file):
                clean_csv_in_chunks(csv_file_dir, name_of_cleaned_file)
                cleaned_a_csv = True
    return cleaned_a_csv

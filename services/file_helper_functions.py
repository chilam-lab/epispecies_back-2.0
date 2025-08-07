from services.clean_csv import clean_csv_in_chunks, create_csv_from_cleaned
from os import listdir, remove
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
    if len(listdir("csv/")) == 0:
        return cleaned_a_csv
    csvs_dir = "csv"
    dir_clean_csv = "cleanedCSV/"
    for file in listdir("csv/"):
        if file.endswith(".csv"):
            csv_file_dir = join(csvs_dir, file)
            name_of_cleaned_file = join(dir_clean_csv, file)
            if exists(name_of_cleaned_file):
                remove(name_of_cleaned_file)
            clean_csv_in_chunks(csv_file_dir, name_of_cleaned_file)
            cleaned_a_csv = True
    return cleaned_a_csv


def get_cleaned_csv_files() -> bool:
    """
    Get all csv files in the cleanCSV directory of the project, then 
    creates a new file that contains all informations of the csvs.

    Returns:
    
    --- True if the file was created successfully. \n
    --- False if directory is empty, no csv files were found or file was not created.
    """
    file_created = False
    if len(listdir("cleanedCSV/")) == 0:
        return file_created
    first_chunk = True
    csv_file_dir = ""
    dir_clean_csv = "cleanedCSV"
    csv_to_table = join(dir_clean_csv, "csv_to_table_file.csv")        
    if exists(csv_to_table):
        remove(csv_to_table)
    for file in listdir("cleanedCSV/"):
        if file.endswith(".csv"):
            csv_file_dir = join(dir_clean_csv, file)
            create_csv_from_cleaned(first_chunk, csv_file_dir, csv_to_table)
            first_chunk = False
    if exists(csv_to_table):
        file_created = True
    return file_created
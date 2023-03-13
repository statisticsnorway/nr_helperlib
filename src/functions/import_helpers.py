# -*- coding: utf-8 -*-
"""
Created on Dec 01 10:34:18 2022

@author: Benedikt Goodman
@email: benedikt.goodman@ssb.no
"""

import os
import pandas as pd
import numpy as np


def check_files(path: str) -> list:
    """Checks if files found at path are files. If they are the function yields
    filenames as list of pathlib path objects"""
    
    return [f for f in Path(path).iterdir() if f.is_file()]

def file_finder(folder_path: str,
                filter_clauses: str = None):
    """
    Searches for sub-strings in filenames a given folder and produces a list of
    filenames that matches patterns

    NOTE: All searches are converted to lowercase

    Parameters
    ----------
    folder : string
        Filepath of directory to search for filenames in

    filter_clauses: list
        list of substrings to search for within list of files in a folder.

    Returns
    -------
    filtered_files: list
        List of files contained in folder.

    """

    if filter_clauses is None:
        filtered_files = check_files(folder_path)
        print('Warning: No filter clauses listed, function will return all files in folder_path directory.')
        return filtered_files

    else: 
        filter_clauses = [str(n).lower() for n in filter_clauses]

        # Get all files in folder as list
        all_files = check_files(folder_path)

        # Turn all_files into a series
        all_files = pd.Series(all_files, dtype=str).str.lower()

        # Filter series based on string_filters, turn back to list
        filtered_files = list(
        all_files[
            all_files.str.contains('|'.join(filter_clauses))
            ]
        )

        # Sort list alphabetically
        filtered_files = sorted(filtered_files)

        return filtered_files


def simple_importer(df:'pd.Dataframe', 
                    path_col:str='path',
                    file_col:str='filename',
                    dir_col:str='directory',
                    filetype:str=None,
                    sep_sign:str=';',
                    encode:str='iso-8859-1'):
    """
    Batch import of data from a set of given paths, folder- and filenames.

    Outputs data as a dictionary with filenames as keys, and dataframes
    as values.

    Input dataframe must have a column with paths, filenames and parent
    directory present.

    Parameters
    ----------
    df : pd.Dataframe
        Dataframe containng path, directory and filename of files for import.
    path_col : str
        Column-name of path column. The default is 'path'.
    file_col : str,
        Column-name of filename column. The default is 'filename'.
    dir_col : str, 
        Column-name of directory column. The default is 'directory'.
    filetype : str,
        Filetype of data to import. The default is None.
    sep_sign : str, 
        Separator sign for columns when importing from csv or txt files.
        The default is ';'.
    encode : str,
        Encoding of source datafile. The default is 'iso-8859-1' (anything from SAS
        will have this encoding).

    Raises
    ------
    AssertionError
        Invalid filetype input.

    Returns
    -------
    data_dict : dict
        Dictionary with foldername, and filename as keys, dataframes as values.

    """
    allowed_filetypes = [
        'csv', 'txt', 'sas7bdat', 'parquet', 'xlsx', 'xls'
        ]

    if filetype not in allowed_filetypes:
        raise AssertionError(f'Filetype specified not allowed. Allowed filetypes: {allowed_filetypes}')
        return

    # Where data will be stored
    data_dict = {}

    for directory in df[dir_col].unique():

        # Subset rows to use for file import
        row_subset = df.loc[df[dir_col] == directory, [path_col, file_col]]

        # Store subset to dict to enable dict comprehension below
        file_path_dict = dict(zip(row_subset[file_col], row_subset[path_col]))

        # Read in data on form {directory : filename : df} for allowed filetypes
        # Makes nested dictionary form
        if filetype in ['csv', 'txt']:
            data_dict[directory] = {
                filename : pd.read_csv(path, encoding=encode,
                                       sep=sep_sign, dtype=str) 
                    for filename, path in file_path_dict.items()}

        elif filetype == 'sas7bdat':
            data_dict[directory] = {
                filename : pd.read_sas(path, encoding=encode) 
                    for filename, path in file_path_dict.items()}

        elif filetype in ['xlsx', 'xls']:
            data_dict[directory] = {
                filename : pd.read_excel(path, encoding=encode) 
                    for filename, path in file_path_dict.items()}

        elif filetype == 'parquet':
            data_dict[directory] = [pd.read_excel(path, encoding=encode) 
                    for path in df[path_col]]

        # Change column names i df columns for df inside nested dict w 3 levels
        data_dict = {
            folder_name: {
                file_name: df.rename(columns=lambda x: x.lower(), inplace=False) 
                       for file_name, df in folder_dict.items()
                       } 
            for folder_name, folder_dict in data_dict.items()
        }

    return data_dict
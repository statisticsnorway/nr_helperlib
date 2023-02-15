# -*- coding: utf-8 -*-
"""
Created on Dec 01 10:34:18 2022

@author: Benedikt Goodman
@email: benedikt.goodman@ssb.no
"""

import os
import pandas as pd
import numpy as np


def check_files(path):
    """Checks if files found by listdir are files, if they are,
    appends them to list"""
    file_list = []
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, str(file))):
            file_list.append(file)
    return file_list


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

def file_batch_importer(
    folder_path: str,
    *filter_clauses: str,
    filetype: str = '',
    attach_years: bool = False,
    year_start: int = np.nan,
    year_stop: int = np.nan,
    file_identifier_func = file_finder,
    store_as: str = 'dict',
    decode: bool = True,
    csv_data_as_string : bool = False
    ):
    """
    Imports batches of datasets from REA, BERKAP etc and outputs data
    as as dict, a list coontaining dataframes, or as a singular dataframe.
    
    Files must be importable as dataframes.
    
    The function can be made to add years as a separate column in imported files
    This functionality does not infer years from filenames, but presumes
    that files have the same naming convention and that NO DUPLICATE FILES ARE 
    PRESENT so that they when sorted in alphabetical order will be in 
    a temporally correct order as well.
    
    A vector of years defined by the user is then attached as an added variable 
    to each imported file. Vector lenght must match number of imported files
    for this functionality to work.

    Parameters
    ----------
    folder_path : str
        Folder to import data from.
        
    *filter_clauses : str
        Substrings to search for in title. If omitted the function will import
        all files of type specified in filetype parameter.
    
    filetype : str, mandatory
        Determines which filetype the function imports from given folder_path.
        The default is None.
        
    attach_years : bool, optional
        Function will attempt to attach years as added variable in datasets if
        set to true. Should only be used when importing many identical datasets
        from different years. The default is False.
        
    year_start : int, optional
        Start point for years to attach as variables in imported datasets.
        The default is np.nan.
        
    year_stop : int, optional
        End point for years to attach as variables in imported datasets.
        The default is np.nan.
        
    file_identifier_func : Function, optional
        Sets which function to use as identifier function for files. 
        Only added to facilitate addition of other identifier functions in the
        future. The default is file_finder.
        
    store_as : str, optional
        Determines which datastructure the function stores imported dataframes
        within. Valid settings are: 
            'list' -> stores dataframes in a list
            'dict' -> stores dataframes in a dictionary with filenames as keys
            'df' -> attempts to concat imported dataframe into one large dataframe,
                    should only be used for similarly structured datasets.
        The default is 'dict'.
        
    decode : bool, optional
        Sets decoding of iso-8859-1 encoded datasets to utf-8 standard to on 
        or off. Many files produced by older SSB systems need this setting
        to be turned on. The default is True.
        
    csv_data_as_string : bool, optional
        Imports all data as strings if enabled when importing csv files.
        This is because csv does not contain datatype information, which leads
        pandas to automatically infer datatypes with varying results. Enabling
        this when importing csv files will make you life easier..
        The default is False.

    Returns
    -------
    df, df_list or df_dict : dataframe, list, or dict
        Imported data as dataframe, a list containing dataframes or a 
        dictionary containing dataframes with filenames as keys. Results follow
        store_as setting.
        
    """
    allowed_filetypes = ['csv', 'txt', 'sas7bdat', 'parquet', 'xlsx', 'xls']
    
    if filetype not in allowed_filetypes:
        print("""
Supported filetypes for data import are csv, parquet, sas7bdat,
txt, xls and xlsx. Please specify which one of these you want to 
import.
        """)
        
        return
    
    # # Defines byte decoder as local variable
    # byte_decoder_func = byte_decoder
    
    # Get files in list, save files to list as local varibles
    files_found = file_identifier_func(folder_path, *filter_clauses)
    
    # Loops through files found, searches for filetype suffix (sas7bdat, csv etc... )
    files_for_import = [elem for elem in files_found if filetype in elem]
    
    # Generate containers to store dataframes in
    df_list = []
    df_dict = {}
    j = 0

    if len(files_for_import) == 0:
        print('Error: No sas-files found in working directory')

    # Triggers import function for all files in list, decodes then stores
    # to list of dataframes
    else:
        for i in files_for_import:
            # import files depending on stated filetype
            if filetype == 'xlsx' or filetype=='xls':
                temp_df = pd.read_excel(folder_path + '/' + files_found[j])
                
            elif filetype == 'csv' or filetype == 'txt':
                temp_df = pd.read_csv(folder_path + '/' + files_found[j], dtype=str)
                
            elif filetype == 'parquet':
                temp_df = pd.read_parquet(folder_path + '/' + files_found[j])
                
            elif filetype == 'sas7bdat':
                # Decodes byte strings: b'old_string' -> 'new string'
                if decode == True:
                    temp_df = pd.read_sas(folder_path + '/' + files_found[j],
                                          encoding='iso-8859-1')
                    
                # Maintains byte encoding -> b'strings look like this'
                elif decode == False:
                    temp_df = pd.read_sas(folder_path + '/' + files_found[j])

                # if decode == True:
                #     temp_df = temp_df.pipe(byte_decoder_func)
            
            # Ensures lowercase strings in column headers
            temp_df.columns = temp_df.columns.str.lower()
            
            if store_as == 'list' or store_as == 'df':
                # Add temp_df to list of dataframes
                df_list.append(temp_df)
                j += 1
            
            elif store_as == 'dict':
                filename = files_for_import[j]
                df_dict[filename] = temp_df
                j += 1
        
        # Attaches years in separate column, drops rows where year isnt present
        if attach_years == True:
            
            error_message = """
            Error: Amount of years added not equal to amount of files collected for import.
                   Ensure correct number by searching for a more specific filename 
                   (i.e. 'nyt1f_hr2019' vs 'nyt1f').
              
              Hint: 
                  attach_years=True should only be used when importing many identical
                  datasets from different years. This error will not show if attach_years=False.
              """
            
            # Creates list of years to attach, loop attaches years as column 
            years = list(np.arange(year_start, year_stop + 1))
            y = 0
            
            # Attaches years to each dataframe in ordered list
            if store_as == 'list' or store_as == 'df':
                if len(years) != len(df_list):
                    print(error_message)
                    return
                for i in years:
                    df_list[y]['aar'] = years[y]
                    y += 1
                    
                    # Delete entries where aar is null
                    #if df_list[y]['aar'].isna().any() == True:
                     #   del df_list[y]
            
            # Attaches years to each dataframe in dict. 
            elif store_as == 'dict':
                if len(years) != len(df_dict):
                    print(error_message)
                    return
                for key in df_dict:
                    df_dict[key]['aar'] = years[y]
                    y += 1
                    
                    # Delete entries where aar is empty
                    #if df_dict[key]['aar'].isna().any() == True:
                        #del df_dict[key]
                
        # exports input data as singular dataframe
        if store_as == 'df':
            # Concat df_list together to a single dataframe, then export
            df = pd.concat(df_list)
            return df 
        
        # exports input datasets as list of dataframes
        elif store_as == 'list':
            return df_list
        # Exports dict
        elif store_as =='dict':
            return df_dict
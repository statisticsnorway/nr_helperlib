# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 10:53:37 2023

@author: Benedikt Goodman
@email: benedikt.goodman@ssb.no
"""

import pandas as pd

from src.functions.import_helpers import simple_importer
from src.functions.logic_helpers import check_listinput

from tqdm import tqdm


class DataImporter():

    def __init__(self, metadata_df, import_func=simple_importer,
                 dataset_list=None):

        self.metadata = metadata_df
        self.import_func = import_func
        self.dataset_list = dataset_list

    def simple_import(self,
                      path_col: str = 'path',
                      file_col: str = 'filename',
                      dir_col: str = 'directory',
                      filetype: str = None,
                      sep_sign: str = ';',
                      encode: str = 'iso-8859-1'):
        """
        Import function based on simple_importer.
        Uses a dataframe containing path, parent directory
        and filename to import datasets within a set of subfolders
        and return them in a nested dictionary."""

        bar = tqdm(range(1), desc='Data load progress')
        # Timing loop with progress bar
        for n in bar:
            self.folder_dict = self.import_func(self.metadata,
                                                path_col = path_col, 
                                                file_col = file_col,
                                                dir_col = dir_col,
                                                filetype = filetype,
                                                sep_sign = sep_sign,
                                                encode = encode)

        return self



    def add_years(self):
        """Method adds years to every dataframe in nested dictionary.
        Infers year to add based on folder names from input data."""

        # Create local copy of dictionary with all data
        df_dict = self.folder_dict

        # Make list from keys, check if they are numeric, if test fails
        # Print error message
        """should change to throw the exact error you want tbf..."""
        # if self.use_numeric_folders is False:
        #     return self

        # else:
        try:
            key_list = pd.Series(list(df_dict.keys())) # Will fail if folders contain non-numeric data

        except ValueError:
            error_message = """
Error: Sub-directory names containing raw data cannot be interpeted as numbers.
Rename folders in main directory to yearly format.
Hint:
    Subfoldername 1 = 2021
    Subfoldername 2 = 2022
    etc...
            """
            print(error_message)

        # Loop below adds years to all dataframes in the entire dict
        """Adds aar column with year equal to sub-folder name"""
        for year in df_dict.keys():
            # Creates a temp dict based on year inferred from keys in dictionary
            temp_dict = df_dict[year].copy()

            # Iterates through each dict, adds year column based on inferred year
            for dataframe in temp_dict.keys():
                temp_dict[dataframe]['aar_added'] = year

            # Add temp_df to output dict
            df_dict[year] = temp_dict

        # Overwrite local variable
        self.folder_dict = df_dict

        return self


    def categorise_data(self, 
                    sort_by='aar_added',
                    dataset_list = None,
                    ):
        """
        Reorganise nested dictionary containing data according to name of 
        dataset.

        Checks values (filenames) of master dictionary with data for string 
        matching the keyword in keyword_list,extracts them and concatenates 
        them to a single dataframe for each category.

        Parameters
        ----------
        sort_by : str
            Dataframe column to sort values in resulting dataframes by.
            The default is 'aar_added'.
        dataset_list : list
            List of keywords to categorise datasets in output dictionary.

        Returns
        -------
        sorted_dataframes : dict
            Dictionary containing dataframes sorted by keywrords provided in 
            dataset_list.

        """
        # Progress bar
        bar = tqdm(range(1), desc='Sorting data by category of dataset')

        for i in bar:
            # Set folder dict variable as local variable
            # This sucker contains the data you want to reorganise
            folder_dict = self.folder_dict

            # For storage of concatenated dataframes
            storage_dict = {}

            for kw_list_item in dataset_list:

                # Emtpy list to store extracted dataframes in
                df_list = []

                for key, val in folder_dict.items():

                    """Extracts folder_dict[key] if it matches the start of item in
                    dataset_list. Returns empty dictionary if not -> hence usage of
                    if conditions below."""
                    rearanged_dict = {k: v 
                                    for k, v in folder_dict[key].items() 
                                    if k.startswith(kw_list_item)}

                    # Catches dicts with content and concatenates to dataframe
                    if len(rearanged_dict) > 0:
                        df = pd.concat(rearanged_dict)
                        df_list.append(df)

                        # Concats list of matching dataframes into one dataframe
                        df_with_filtered_keys = (
                            pd.concat(df_list)
                            .reset_index(level=0)
                            .rename(columns={'level_0': 'filename'})
                            .sort_values('aar_added'))

                        storage_dict[kw_list_item] = df_with_filtered_keys

                    error_msg = 'No categories matching imported data found in dataset_list. Please revise dataset_list items.'

                if len(storage_dict) == 0:
                    raise AssertionError(error_msg)

            self.sorted_dataframes = storage_dict

        return self

    def __add_energiregnskapet(self):
        # Add in
        self.sorted_dataframes['energiregnskapet'] = self.energiregnskapet
        
        return self


    def year_duplicate_tidy_func(self,
                                 dupl_col1=None,
                                 dupl_col2=None):
        """
        Function that eliminates dual existence of dupl_col1 and dupl_col2 
        variables in imported dataframes.

        If both exist the function removes dupl_col2. If dupl_col2 doesnt exist 
        then dupl_col2 is renamed to dupl_col1.

        Parameters
        ----------
        storage_dict : Dictionary
            Dictionary containing dataframes as keys with possible dual existance
            of variables 'aar_added' and 'aar'
        dupl_col1 : Dataframe column
            Duplicate column 1 to search for duplicate values in
        dupl_col2 : Dataframe column
            Duplicate column 2 to search for duplicated values

        Returns
        -------
        storage_dict : Dictionary
            Dictionary containing dataframes as keys. Dual vars have been eliminated

        """
        storage_dict = self.sorted_dataframes

        for df in storage_dict.keys():
            column_names = storage_dict[df].columns

            # Prompts warning
            if dupl_col1 and dupl_col2 not in column_names:
                print(f'Warning: Specified duplicate columns not found in {df}')
                pass

            # Renames dupl_col2
            elif dupl_col1 not in column_names:
                storage_dict[df] = storage_dict[df].rename(
                    columns={dupl_col2 : dupl_col1})

            # Drops dupl_col2 if dupl_col1 is present
            else:
                storage_dict[df] = storage_dict[df].drop(dupl_col2, axis=1)

        self.sorted_dataframes = storage_dict

        return self

    def sort_df(self, sort_by: 'list or str' = None):
        """
        Sort all dataframes stored in a dictionary by given value(s) if
        all user-defined columns to sort by exist in dataframe

        Parameters
        ----------
        sorted_dataframes : dict
            Dictionary containing dataframe. Variable of ImportClass.

        sort_by : 'list or str', optional
            Values to sort dataframe by. The default is None.

        Returns
        -------
        sorted_dataframes : dict
            Writes dictionary to class memory.
        """
        # Extract subset where columns sortedby are not present
        _exempt_dfs = {
            key: df for key, df in self.sorted_dataframes.items()
            if not all(item in df.columns for item in sort_by)
        }
        # Dfs where columns sorted by are present
        _sorted_dfs = {
            # Sorts dataframes in dict by values
            key: df.sort_values(sort_by) for key, df 
            in self.sorted_dataframes.items() 

            # Checks if all items in sort_by are present in df columns
            if all(item in df.columns for item in sort_by)
        }

        # Add extempt to sorted
        _sorted_dfs.update(_exempt_dfs)
        
        if self.sorted_dataframes.keys() == _sorted_dfs.keys() is False:
            msg = [
                'Warning!',
                'sort_df method accidentally removed some dataframes.',
                'The following dataframes were eliminated:']
            print(' '.join(msg))
            [key for key in _sorted_dfs if key not in self.sorted_dataframes.keys()]
        
        self.sorted_dataframes = _sorted_dfs

        return self

    # Should this maybe be generalised?
    def subset_years(self, df_name=None, year_col='aar', start_year=2010, stop_year=None):
        """Method for filtering out all years aside from desired year from energiregnskapet."""

        if df_name is None:
            print('Please define which dataframe you want to subset. Hint: it is one of the names you defined in dataset_list.')

        # Write dictionary with data as local variable
        __subset_df = self.sorted_dataframes[df_name].copy(deep=True)
    
        # Subset dataframe with energiregnskapet to only contain data between 
        # start_year and last_year
        __subset_df = (__subset_df.loc[
                                (__subset_df[year_col] >= start_year) &
                                (__subset_df[year_col] <= stop_year)
            ]
        )
        
        # Write changed dictionary to object variables
        self.sorted_dataframes[df_name] = __subset_df

        return self
    
    # Output module
    def write_data(self, output_object: str = 'folder_dict'):
        """Write dictionary inside object memory as variable"""

        if output_object == 'folder_dict':
            return self.folder_dict
        if output_object == 'sorted_df':
            return self.sorted_dataframes
        else:
            print('Invalid input. Write folder_dict for datasets organised by subfolder or sorted_df for datasets sorted by keywords.')
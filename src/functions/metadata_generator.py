# -*- coding: utf-8 -*-
"""
Author: Benedikt Goodman
Email: bgo@ssb.no
Created: 02.12.2022

"""

import os
import pandas as pd

from src.functions.logic_helpers import check_listinput
from src.functions.utility_module import input_argument_none_eliminator
from src.functions.import_helpers import file_finder

class FolderSearcher():
    """
    Class for identifying files within a folder hierearchy according to a
    set of search terms and datatype criteria.
    """

    def __init__(self, filepath:str,
                 dataset_list:list = None,
                 datatype: str = None,
                 use_numeric_folders: bool = True,
                 list_check_func:'function' = check_listinput
                 ):
        """
        Initalisation function accepting class attributes.

        You can also define which datasets to import by providing a list of
        dataset names. The behaviour of the search works according to an
        additive filter logic. I.e. more search terms -> more results.

        Parameters
        ----------
        filepath :  str
            Directory to search for subfolders containing sas7bdat datasets
            to import.
        dataset_list : list
            List of strings containing names of datasets to import from
            filepath and respective subfolder heirearchy.
        datatype : str
            String containing datatype of files to import. Supported
            datatypes are sas7bdat, csv, excel, txt and parquet
        use_numeric_folders : bool
            Class will only import data from folders that have names which
            are convertible to numbers.
        list_check_func : function
            Function that checks for non-emptyness in a lsit or whether the list
            contains anything else than strings, floats and ints. If input is not
            list, contains allowed datatypes or is empty, the function will raise 
            errors.
        """
        # Typecheck for type, lenght and content of dataset_list
        list_check_func(dataset_list, 'dataset_list')

        # Filepath = directory where sub-folders with data exists.
        self.path = str(filepath)
        self.dataset_list = [str(item).lower() for item in dataset_list]
        self.datatype = str(datatype)
        self.use_numeric_folders = bool(use_numeric_folders)

        # Variables to store metadata, and imported files
        self.list_of_folders = ()

        # user defined input variables
        self.start_year = int(input('Start year for data import:'))
        self.stop_year = int(input('Last year for data import:'))

    def read_folders(self):
        """
        Identifies subfolders present in chosen directory.
        Makes tuple of said subfolders.

        Returns
        -------
        self._ of_folders : tuple
            Tuple of folders inside specified directory.

        Raises
        -------
        ValueError: When input filepath from user doesnt exist.
            Python will not provide user with useful feedback otherwise
            (there's no string for this type of error it seems.)
        """
        try:
            self.list_of_folders = tuple(next(os.walk(self.path))[1])
        #Catches annoying error 
        except StopIteration:
            raise ValueError('Input path does not exist.')

        [string.lower() for string in self.list_of_folders]

        return self.list_of_folders

    def __filter_years(self, file_list:list, start_year:int, stop_year:int):
        """Filter list of years by start and stop year"""

        if self.use_numeric_folders is not True:
            return file_list

        else:
            __year_vector = pd.Series(data=file_list).astype(int)

            # .loc filters for start and stop
            __filtered_years = list(
                __year_vector.loc[
                    ((__year_vector >= start_year) & (__year_vector <= stop_year))
                ]
            )

            return __filtered_years


    def __check_numeric_folders(self):
        """
        Iterates through a list of strings, filters out non_numeric folders
        Also calls on __filter_years() method to eliminate years outside of 
        start and stop years.

        Returns
        -------
        self.__numeric_folders
            Filtered, numerical list of folder in which data resides.
        """
        self.__numeric_folders = []

        # Filter out non-numerically named folders
        [self.__numeric_folders.append(item)
         for item
         in self.list_of_folders
         if item.isnumeric()]

        self.__numeric_folders = self.__filter_years(
            self.__numeric_folders,
            self.start_year,
            self.stop_year,
            )

        return self.__numeric_folders


    def __read_filepaths(self, select_numeric_folders: bool= None):
        """
        Creates filepaths based on filepath and subfolders detected by
        .read_folders() method.

        Eliminates non-numeric folders from list of folders if
        use_numeric_folders is enabled.

        Parameters
        ----------
        select_numeric_folders : Bool, optional
            If set to true, the function will call on a helper function to
            filter out non-numerically named folders. The default is True.

        Returns
        -------
         List
            List of filepaths for all detected files

        Raises
        ------
        AssertionError:
            Raised if dataset_list yields no matches with files found
            in subfolders searched.

        """

        # Temp storage, used for listcomps below
        self.filepaths = []

        # Generate input list of files
        self.folder_list = self.read_folders()

        # Keeps only numerically named folders if true
        if select_numeric_folders is True:
            self.folder_list = self.__check_numeric_folders()

        # Generate path from folder and file information
        for index, year in enumerate(self.folder_list):

            # Generate list of files within each sub-folder
            __files_in_subfolder = file_finder(
                f'{self.path}/{year}',
                filter_clauses=self.dataset_list)

            # Append full filepath to list
            [self.filepaths.append(f'{self.path}/{year}/{file}') 
            for file in __files_in_subfolder]

        error_string = "Search terms in dataset_list yielded no results. Please revise search terms."

        if len(self.filepaths) <= 0:
            raise AssertionError(error_string)

        return self.filepaths

    @staticmethod
    def __string_clipper(target_iterable:iter= None, elem_num:int =None, sep='/'):
        """
        Split string into list, extracts element n given by elem_num.

        Primarily used for splitting an item off a filepath string.

        The method follows normal pythonic slicing-rules.

        Parameters
        ----------
        target_iterable : iterable
            pd.Dataframe column, list or similar iterable item. Must contain
            strings to split.
        elem_num : int, optional
            Element in list of substrings to keep. The default is None.
        sep : string
            Separator used to split string into list items. The default is '/'.

        Returns
        -------
        list
            List, of the same lenght as input list
            of desired substrings

        """
        # Type checks
        # if isinstance(target_iterable, type(iter)) is False:
        #     raise TypeError('Target_iterable must be an iterable item (list, dict, pd.Series etc.)')
        if isinstance(elem_num, int) is False:
            raise TypeError('elem_num must be an integer')
        elif isinstance(sep, str) is False:
            raise TypeError('sep must be a string')

        return [item[elem_num] 
                for item 
                in target_iterable.str.split(sep)]


    def __generate_metadata_df(self):
        """
        Generate pandas dataframe containing filenames, directory and path

        Calls on:
            -__read_filepaths() -> makes list of filepaths, if use_numeric_
            folders is set to True for the class it will only return numerically
            named folders.
            - __string_clipper() -> cuts off elements in given strings
            to only show certain elements. For isolating filenames, parent-
            folders of files etc.

        Returns
        -------
        self.metadata_df : pd.Dataframe
            Dataframe containing metadata 

        """
        # Generates filepaths
        # use_numeric_folders variable sets behaviour for whether only numeric
        # folders are imported or not
        __filepaths = self.__read_filepaths(
            select_numeric_folders=self.use_numeric_folders)

        # Add filepaths and filenames as columns in metadata
        self.metadata_df = pd.DataFrame(
            __filepaths, 
            columns=['path']).astype(str)

        # Stores filenames from paths as new column
        self.metadata_df['filename'] = self.__string_clipper(
            target_iterable = self.metadata_df['path'],
            elem_num=-1,
            )

        # Gets directory location of files
        self.metadata_df['directory'] = self.__string_clipper(
            target_iterable = self.metadata_df['path'],
            elem_num=-2,
            )

        return self.metadata_df

    def generate_metadata(self):
        """
        Make and display names and directories of files identified for import.

        Calls on:
            - __generate_file_info() -> which generates the dataframe

        Returns
        -------
        metada_df : Dataframe
            Dataframe containing file metadata from identified files
            The method returns self so it can be chained.

        """
        self.__generate_metadata_df()

        return self

    @input_argument_none_eliminator
    def subset_datasets(self, 
                        groups:list= None, 
                        keep_vals:list = None, 
                        group_col:'list or str'= 'filename',
                        val_col:'list or str'= 'directory'):
        """
        Keep only given subsets of values from a group of data in
        a dataframe.

        I.e. if group_cop = filename and val_col = directory the subsetter
        will filter out values not specified in keep_vals list. This allows
        for filtering away certain years for a given datatype when making 
        the metadata_df.

        Parameters
        ----------
        groups : list
            Groups of data to preform subsetting on. The default is None.
        keep_vals : list
            List of values to keep within subset groups. The default is None.
        group_col : 'list or str'
            Column to identify subset groupings in. The default is 'filename'.
        val_col : 'list or str'
         Column to preform filtering by values on. The default is 'directory'.

        Returns
        -------
        metadata_df : dataframe
            Outputs filtered dataframe. The method returns self so it can be
            chained.
        """
        # Filters out desired subset by column, groups and values
        subset = self.metadata_df.loc[
            (self.metadata_df[group_col].str.contains('|'.join(groups))) &
            (self.metadata_df[val_col].str.contains('|'.join(keep_vals)))
            ]

        # Drops what we dont want from output df
        self.metadata_df = (
            self.metadata_df.loc[~self.metadata_df[group_col].str.contains('|'.join(groups))]
            )

        # Sticks what we want on output df
        self.metadata_df = pd.concat([self.metadata_df, subset])

        return self

    def output_df(self):
        """Outputs metadata_df as dataframe"""
        return self.metadata_df.sort_values('filename')
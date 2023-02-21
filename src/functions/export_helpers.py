# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 09:12:18 2023

@author: Benedikt Goodman
@email: benedikt.goodman@ssb.no
"""

import os
import pandas as pd

"""Add in more advanced folder check. It should be able to deal with
an incomplete set of folders and then create those which are missing"""

def batch_exporter(df, year_col: str = None, export_path: str = None,
                   prefix: str = None):
    """
    Reads in dataframe, identifies year column and exports data to a set of
    folders that equal the amount of years in the dataframe. If these folders
    do not exist they will be created. 

    Data is then exported to each folder based on which year it belongs to.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe for export.
    year_col : str
        Column indicating years in dataset. The default is None.
    export_path : str
        Path to subfolders in which data will be exported to. 
        The default is None.
    prefix : str
        Prefix in name of dataset. The default is None.

    Returns
    -------
    None.

    """
    # Yields True if any subfolder already exists, then wont create folders
    if all(
        # path exists yields boolean
        [os.path.exists(f'{export_path}/{year}')
         for year in df[year_col].unique()]
    ) is False:
        # Make folders for unique years present
        [os.makedirs(f'{export_path}/{year}') for year in df[year_col].unique()]

    # Export data for unique years present
    [(df.loc[df[year_col] == year]
        .to_excel(f'{export_path}/{year}/{prefix}_{year}.xlsx', index=False))
        for year in df[year_col].unique()
            ]
    
    return
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 09:20:40 2022

@author: bgo
"""

import numpy as np
import pandas as pd

# # Disable user warnings from df_generator function
# import warnings
# warnings.simplefilter(action='ignore', category=UserWarning)

def df_generator(n_sectors, date_start, date_stop, frequency, 
                 null_value_ratio=0.1, insert_null_values=False):
    """
    Generate dataframe indexed by period with n timeseries that have trend
    and random noise. Can fill dataframe with a proportion of null values
    in random places if insert_null_values = True.
    
    Input arguments are positionally sensitive

    Parameters
    ----------
    date_start : string
        First date in generated dataframe. Format YYYY-MM-DD
    date_stop : string
        Last date in generated dataframe Format YYYY-MM-DD
    frequency : string
        Frequency of dates generated. Day(D), month(M), quarter(Q)
        and year(Y) are valid inputs period
    null_value ratio : int
        Decides ratio of null values in dataframe
    insert_null_values : bool
        True = null values will be inserted randomly
        False = no null values will be inserted

    Returns
    -------
    df : Dataframe with random data for n time series indexed by date

    """
    # Generate pd series with dates
    date_vector = pd.date_range(
        start=date_start, end=date_stop, freq=frequency, 
        )
    
    # Set random seed
    np.random.seed(0)
    
    # Generate noise matrix
    noise = np.random.normal(0,2, size=(len(date_vector), n_sectors))
    
    # Generate dataframe with random data with same lenght as date vector
    # Creates 99 columns of random data
    df = pd.DataFrame(
        np.fromfunction(lambda i, j: (i + j**4) - j*2, dtype=float,
            shape=(len(date_vector), n_sectors),
            )
        )
    
    # Add random noise to timeseries
    df = df + noise
    
    # Adds suffixes to column names
    df = df.add_prefix('industry_')

    # Adds datetime and month column
    df['date'] = date_vector
    df['date'] = df['date'].dt.to_period(frequency)
    
    # Aestethic ops
    df = df.set_index(df['date']).drop('date', axis=1)
    # df = df.iloc[:, 0:3]
    
    # Generate dataframe with random null values
    if insert_null_values == True:
        for col in df.columns:
            df.loc[df.sample(frac=null_value_ratio).index, col] = np.nan

    return df
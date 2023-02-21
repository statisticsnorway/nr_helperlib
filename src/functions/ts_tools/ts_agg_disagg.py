# -*- coding: utf-8 -*-
"""
Created on Thu Aug 25 16:58:10 2022

@author: bgo
"""

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
import os

def disagg_func_stairs(df, input_freq, output_freq):
    """
    Function that returns a disaggregated version of input dataframe.
    The disaggregation method applied takes values observed in each
    parent time-period and copies it to each of the child time-periods.

    Example
        Original data           |timeperiod||value|
                                |  2020Q1  ||1900|

        Transformed data       |timeperiod||value|
                               |  2020M1  ||1900|
                               |  2020M2  ||1900|
                               |  2020M3  ||1900|

    The function can only disaggregate from years to quarters or from
    quarters to months.

    It requires period-indexes to work.

    Parameters
    ----------
    df : dataframe
        Input dataframe to be disaggregated.
    input_freq : string
        String specifying periodicity in index of input dataframe.
        Accepts A = annual and Q = quarterly
    output_freq : string
       String specifying periodicity in index of output dataframe.
       Accepts A = annual and Q = quarterly

    Returns
    -------
    df : dataframe
        Disaggregated version of input dataframe of desired periodicity.

    """

    # Check for datatype of input dataframe
    if type(df).__name__ != 'DataFrame':
        raise TypeError('df must be a dataframe')

    # Define allowed index types, only period indexes of annual and quarterly
    # allowed
    allowed_index_types = [
        'period[Q-DEC]', 
        'period[A-DEC]',
        ]

    # Check for allowed datatypes in index
    if df.index.dtype not in allowed_index_types:
        raise IndexError('dataframe must have PeriodIndex with frequency Q or Y')
        return

    # Branch for quarter to monthly disaggregation
    if input_freq == 'Q' and output_freq == 'M':

        # Check for index dtype
        if df.index.dtype == 'period[Q-DEC]':

            # Branch containing disaggregation method
            try:
                # Ensure all values are numeric
                df = df.astype(float) 

                # Resampling via forwardfilling - identifies value per period
                # Then fills that value across resampled period
                df = df.resample(rule=output_freq, convention='start').ffill()

                return df

            # Raise issue if test fails
            except TypeError:
                raise TypeError('Dataframe contains non-numeric datatypes \n'
                                'which cannot be converted to numeric. Check \n'
                                'dataframe datatypes with df.info() or df.dtypes')

            except ValueError:
                raise ValueError('Dataframe contains non-numeric datatypes \n'
                                 'which cannot be converted to numeric. Check \n'
                                 'dataframe datatypes with df.info() or df.dtypes')

        elif df.index.dtype != 'period[Q-DEC]':
            raise IndexError('Input dataframe index not of quarterly periods (i.e. period[Q-DEC])')

    # Branch for yearly to quarterly disaggregation
    elif input_freq == 'Y' and output_freq == 'Q':

        # Check for index dtype
        if df.index.dtype == 'period[A-DEC]':

            try:
                # Ensure all values are numeric
                df = df.astype(float) 

                # Resampling via forwardfilling - identifies value per period
                # Then fills that value across resampled period
                df = df.resample(rule=output_freq, convention='start').ffill()

                return df

            except TypeError:
                raise TypeError('Dataframe contains non-numeric datatypes \n'
                                'which cannot be converted to numeric. Check \n'
                                'dataframe datatypes with df.info() or df.dtypes')

            except ValueError:
                raise ValueError('Dataframe contains non-numeric datatypes \n'
                                 'which cannot be converted to numeric. Check \n'
                                 'dataframe datatypes with df.info() or df.dtypes')

        elif df.index.dtype != 'period[A-DEC]':
            raise IndexError('Index not of annual periods (i.e. period[A-DEC])')

    else:
        raise ValueError('Function can only convert from yearly (Y) to quarterly (Q) \n'
              'or quarterly (Q) to monthly (M)')

#%%

def aggregation_func(df, target_freq, aggregation_method, ignore_incomplete=True):
    """

    Aggregation function that outputs mean or sum of input values. Works for
    however many sectors you want.

    It can only aggregate. I.e. the function will return errors
    if desired output periodicity is higher than the input.

    The function has two aggregation methods, sum and mean. These
    are similar to FAME's SUMMED and AVERAGED aggregation techniques.

    sum will take the value of each input period and output the sum of these
    within each aggregated group of periods.

    mean will output the average across grouped input periods.

    Parameters
    ----------
    df : Pandas dataframe.
        Contains data to be resampled.
    target_freq : String.
        Desired frequency of output dataframe. Input as string.
            'M' = months
            'Q' = quarter
            'Y' = year
    aggregation_method: String.
        Decides aggregation method. Valid inputs are 'mean' or 'sum'.
    ignore_incomplete: boolean.
        True = function will not aggregate periods containing missing values.
        False = function aggregates all periods, but will output a warning if
                missing values are detected
    Returns
    -------
    df : Pandas dataframe.
        Aggregated dataframe

    """
    # Creates local boolean variable to test if df is a dataframe
    is_df = type(df).__name__ == 'DataFrame'

    # List of allowed index datatypes
    allowed_dtypes = [
        'datetime64[ns]',
        'period[D]',
        'period[M]',
        'period[Q-DEC]',
        'period[A-DEC]',
        ]

    # List of allowed frequency inputs
    allowed_freqs = ['M','Q','Y']

    # define allowed aggregation methods
    allowed_methods = ['mean', 'sum']

    # Define dict of allowed combos, then reverse keys and values
    allowed_combos = { i : allowed_dtypes[i] for i in range(0, len(allowed_dtypes)) }
    allowed_combos = {v: k for k, v in allowed_combos.items()}

    # Extract input index type, then assign to values in dict
    input_idx_type = df.index.dtype
    input_idx_num = allowed_combos[input_idx_type]

    # assign values to allowed target_frequencies
    target_freq_vals = {'D': 1, 'M' : 2, 'Q' : 3, 'Y' : 4 }
    target_freq_num = target_freq_vals[target_freq]

    ## Conditions for error messages
    # df not a df
    if is_df == False:
        raise TypeError('df is not a dataframe')
        return

    # target_freq input not in list of viable inputs
    elif target_freq not in allowed_freqs:
        raise ValueError("Invalid target_freq input. Valid inputs are 'M' \n,"
                         "'Q' and 'Y'.")
        return

    # aggregation method not sum or mean
    elif aggregation_method not in allowed_methods:
        raise ValueError('Invalid aggregation_method input. Valid methods are \n'
                         "'mean' and 'sum'.")
        return

    # ignore incomplete not of boolean type
    elif type(ignore_incomplete) != 'bool' == False:
        raise ValueError('Invalid ignore_incomplete input. Valid input type is \n'
                         'boolean. I.e. this argument needs to be set to True \n'
                         'or False')
        return

    # incorrect index type on input df
    elif df.index.dtype not in allowed_dtypes:
        raise IndexError("Invalid index type in input dataframe. Supported \n"
                         "indexes are currently datetime64[ns] and period.")
        return

    # raise error if target_freq value is lower than input index value
    if target_freq_num <= input_idx_num:
        raise IndexError('Input dataframe index frequency lower than desired output frequency. \n'
                         'Make sure index is of a more frequent periodicity than desired output \n'
                         '\n'
                         "Example: \n"
                         "quarters ('Q') -> year ('Y') OK \n"
                         "quarters ('Q') -> months ('M') not OK \n")

    # Conducts test of input argument variable types
    # It checks if df is a dataframe, and that arguments are strings
    if is_df is True and type(target_freq) is str and target_freq in allowed_freqs:

        # Path taken if periods with nulls are NOT to be aggregated
        if ignore_incomplete == True:

            if aggregation_method == 'mean':

                # Helper function to detect null values
                def smart_mean(series):

                    # Checks for nulls in input columns
                    if any(pd.isnull(series)):
                        return np.nan
                    else:
                        return series.mean()

                df = df.resample(target_freq).apply(smart_mean)
            

            elif aggregation_method == 'sum':

                # Helper function to detect null values
                def smart_sum(series):

                    # Checks for nulls in input columns for target_freq window
                    if any(pd.isnull(series)):
                        return np.nan

                    # If no nulls present, take mean
                    else:
                        return series.sum()

                # Applies function to entire dataframe
                df = df.resample(target_freq).apply(smart_sum)

            return df

        elif ignore_incomplete == False:
            
            # Local null value check variable
            null_values = df.isnull().values.any()

            if aggregation_method == 'mean':
                # Aggregates to desired frequency
                df = df.resample(target_freq).mean()

            elif aggregation_method == 'sum':
                # Aggregates to desired frequency
                df = df.resample(target_freq).sum()

            # Alerts user if null values are present in dataframe
            if null_values == True:
                print("Null values present in output dataframe. \n"
                      "Ensure that output dataframe contains desired output \n"
                      )

                return df

            return df

    else:
        pass

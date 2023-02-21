"""
Author: Benedikt Goodman
Email: benedikt.goodman@ssb.no
Created: 05.02.2022

**info om funksjonene og programmet**

Note: Der hvor man ser @input_argument_none_eliminator så går funksjons-input
igjennom en funksjon som sjekker om input argumentene til funksjonen under.
I.e. det er en veldig kort måte å bruke en funksjon på en annen funksjon og
i dette tilfellet så utfører denne input sjekk og sier ifra dersom man lar
input argumenter være tomme i funksjonen denne brukes på.

"""


import pandas as pd
import numpy as np
import os
from src.functions.utility_module import input_argument_none_eliminator


@input_argument_none_eliminator
def multiplier(df_in:'pd.DataFrame', 
               X:str = None,
               Y:str = None,
               new_col:str = None):
    """
    Multiplies column X with column Y in df. Results are stored in df[new_col]
    
    new_col = df[X] * df[Y]

    Parameters
    ----------
    df : Dataframe
        Input dataframe.
    X : Dataframe column
        Numerical column values for multiplication.
    Y : Dataframe column
        Numerical column values for multiplication.
    new_col : Dataframe column
        Column with results.

    Returns
    -------
    df : Dataframe
        Dataframe with result column.

    """
    df = df_in.copy()
    df[new_col] = np.multiply(df[X], df[Y])
    return df

@input_argument_none_eliminator
def divider(df_in:'pd.DataFrame',
            X:str = None,
            Y:str = None,
            new_col:str = None):
    """
    Divides column X with column Y in df. Results are stored in df[new_col]
    
    new_col = df[X] / df[Y]
    
    Will output 0 if Y is 0 to avoid zero-division errors

    Parameters
    ----------
    df : Dataframe
        Input dataframe.
    X : Dataframe column
        Numerical column values for division.
    Y : Dataframe column
        Numerical column values for division.
    new_col : Dataframe column
        Column with results.

    Returns
    -------
    df : Dataframe
        Dataframe with result column.

    """
    df = df_in.copy()
    df[new_col] = np.divide(df[X], df[Y], 
                            out=np.zeros_like(df[X]),
                            where = df[Y] != 0)
    return df

@input_argument_none_eliminator
def proportion_func(df_in:'pd.DataFrame',
                    X:str = None,
                    Y:str = None,
                    Z:str = None,
                    new_col:str = None):
    """
    Generates new_col according to:
        new_col = (df[X]/df[Y]) * df[Z]

    Parameters
    ----------
    df : Dataframe
        Input dataframe.
    X : string
        Name of column to designate as X. Numerator in fraction. The default is None.
    Y : string
        Name of column to designate as Y. Denominator in fraction. The default is None.
    Z : string
        Name of column to designate as Z. Muliplies fraction. The default is None.
    new_col : string
        Name of new column generated.

    Returns
    -------
    df : Dataframe
        Output dataframe containing results.

    """
    df = df_in.copy(deep=True)
    df[new_col] = (np.divide(df[X], df[Y],
                             out=np.zeros_like(df[X]),
                            where = df[Y] != 0) * df[Z])
    df[new_col] = df[new_col].round(0)

    return df

# Function for dropping filename from dataframe
# For filling inn unntak
def unntak_filler(df: pd.DataFrame, col='unntak'):
    """
    For setting unntak = 1 in dataframes where unntaksatalog doesnt cover all 
    industries. Col argument sets column to apply fillna action on.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe with unntakskatalog column.
    col : pd.Dataframe column
        Column to apply fillna action. The default is 'unntak'.

    Returns
    -------
    df : pd.DataFrame
        Output dataframe

    """
    df[col] = df[col].copy().fillna(1)
    return df


@input_argument_none_eliminator
def make_sum_column(df:'pd.DataFrame',
                    group: str = None,
                    target_col: str = None,
                    new_col: str = None
                   ):
    """Caculates sum by of target column by group (products, years etc.) and 
    sets new column as sum per group"""
    df[new_col] = df.copy().groupby(group)[target_col].transform('sum')
    return df


@input_argument_none_eliminator
def column_monovalue_checker(df: 'pd.DataFrame', 
                             column: str, 
                             subset_value: 'int, str or float'):
    """
    Checks if all values in a column are equal to subset value. Throws error
    if more than a unique value is observed in a dataframe column. Used for
    Checking integrity of joined dataframes as unsuccesful joins at the very
    least will produce original values and nan-values.

    Leaves input data unchanged.

    Parameters
    ----------
    df : Dataframe
        Input dataframe.
    column : Dataframe column
        Dataframe column to provide check on.
    subset_value : string or value
        Value to check if unique.

    Returns
    -------
    df : Dataframe
        Input dataframe.

    """
    if df[column].unique() == subset_value:
        print(f'Product dataset only caintains {subset_value}: True')
        return df

    else:
        raise ValueError('Error: More than one group of values observed in column. Check for nans and or other errors')
        return

# Trenger bedre navn...
@input_argument_none_eliminator
def data_integrity_checker(input_df: 'pd.DataFrame', 
                           path: str = None,
                           input_col: str = None,
                           source_col: str= None,
                           subset_year: int = 2021,
                           convert_dtype: bool = True,
                           datatype: type = float):
    """
    Checks if a column in input dataframe is identical to the same column in
    a given excel file from control dataset.

    Parameters
    ----------
    input_df : Dataframe
        Input dataframe.
    path : String
        Path of input file to import.
    input_col : string
        Column in input dataframe to check for equality against source data.
    source_col : string
        Column in source data to check against input dataframe column produced by
        python routine.
    subset_year : int
        Year in data to check for equality within. Default value is 2021.
    convert_dtype : boolean
        If set to True the function will convert the input_col and source_col to
        a user defined data type. The default is True.
    datatype : datatypes (int, float, str etc.)
        What datatype to convert values in input and source data columns to.
        The default is float.

    Raises
    ------
    Exception
        Raises exception if data in input and source data columns are not equal.

    Returns
    -------
    input_df : Dataframe
        Same as input dataframe.

    """

    # Converts datatypes in columns to check for equality
    if convert_dtype is True:
        # Raises non-significant error
        input_df.loc[:, input_col] = input_df[input_col].astype(datatype)
        control_data = pd.read_excel(sas_data_file, dtype={source_col : datatype})

    # Regular data import
    elif convert_dtype is False:
        control_data = pd.read_excel(sas_data_file)

    # Condition is tue when values from input df and control data are identical
    condition = (input_df.loc[
        input_df['aar'] == subset_year, 
        input_col].values  == control_data[source_col].values
        )

    if condition is False:
        raise Exception('Input dataframe column not identical to source_data column')
        return

    elif condition is True:
        print(f'Result of avigtsbelagt mengde identical to SAS routine: {condition.all()}')
        return input_df



@input_argument_none_eliminator
def merge_func(df_l: 'pd.DataFrame',
               df_r: 'pd.DataFrame',
               subset_columns_r: list = None,
               join_on: list = None,
               join_method: str = 'left',
               fill: bool = True,
               fill_target_col: str = 'ytart',
               fill_based_on: str = 'produktkode'):
    """
    Merges df_r onto df_l via a conventional left join. Built on pd.merge()
    and will thus exhibit the same behaviour.

    Documentation for pd.merge:
        https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html

    IMPORTANT:
    The function is also able to fill columns in output dataframe based on 
    correspondences in df_r.
    Example:
        - ytart is only present in df_r and is joined onto df_l
        - rows in df_r are much fewer than df_l and consequently ytart
          will default to missing values wherever df_l has a row, but df_r doesn't

    The function solves this problem by mapping out correspondences between ytart
    and product codes, in df_r. So a long as df_r contains all correspondences between
    ytart and product-codes it will automatically fill in the correct ytart in the
    resulting dataframe post-merge.


    Parameters
    ----------
    df_l : Dataframe
        Left dataframe
    df_r : Dataframe
        Right dataframe
    subset_columns_r : string or list of strings
        Columns from df_r to join onto df_l. If no subset is defined 
        the function defaults to joining all functions onto left.
        The list is empty by default.
    join_on : list of strings
        Columns in df_l to join df_r onto. These columns must be present in both
        dataframes. The list is empty by default.
    join_method : string, optional
        Method of joining to happen. Supports left, right, inner and outer. 
        The default is 'left'.

    Returns
    -------
    merged_df : Dataframe
        Merged dataframe with df_l and columns from df_r.

    """
    if subset_columns_r is None:
        subset_columns_r = df_r.columns

    elif join_on is None:
        print('Error: join_on argument is empty. Please define which column you want to join on.')
        return None

    # Merges dataframes
    merged_df = pd.merge(
        df_l.copy(), df_r[subset_columns_r].copy(), on=join_on, how=join_method
    )
    
    
    """
    Infers which ytart and products belong together based on input dataframe if 
    set to True
    """
    if fill is True:
        # Creates associations
        ytart_dict = dict(zip(df_r[fill_based_on], df_r[fill_target_col]))
        # Makes col based on associations 
        merged_df[fill_target_col] = merged_df[fill_based_on].map(ytart_dict)


    return merged_df


@input_argument_none_eliminator
def subsetter(data: dict,
              key: 'name of dataset in dictionary as str' = None, 
              var: str = None,
              value: 'float or int or str' = None,
              dtype_convert: bool = False,
              **dtype_kwargs: 'var_name = data_type'):
    """
    Function that selects a given key in the dictionary containing all
    data then a subset using df.loc to identify a column where variable
    is equal to the value defined by the user.

    Similar to df.loc[df[column] == value] in pandas.

    Parameters
    ----------
    data : Dictionary
        Dictionary containing all datasets. Dataset names = keys, datasets = 
        values. Can be access by data[key].
    key : string
        Name of dataset to subset from.
    variable : String
        Variable to filter dataset by.
    value : string or int
        Value to filter variable by.
    dtype_convert : Boolean, optional
        If set to true, function will attempt to convert variable to given 
        datatype. The default is False. 
    **dtype_dict : keyword arguments.
        Creates a dict of input keyword arguments that is used to define 
        datatypes. Syntax is: column_name = datatype (int, str, float etc.)

    Returns
    -------
    subset : Dataframe
        Filtered subset of your input dataframe

    """
    subset = data[key].loc[data[key][var] == value].copy()

    if dtype_convert is True:
        # Change specified columns to desired datatype
        subset = subset.astype({**dtype_kwargs})

    return subset


@input_argument_none_eliminator
def multi_subsetter(data: dict,
              key: 'name of dataset in dictionary as str' = None, 
              **criteria):
    """
    Function that selects a given key in the dictionary containing all
    data then a subset using a set of keyword arguments to subset the 
    dataset.
    
    Syntax:
        multi_subsetter(data, key='some_key', 
        var1 = 'value1',
        var2 = 'value2',
        var3 = 'value3',
        etc...)
    
    TLDR:
        Built on df.query, translates kwargs into a dictionary and
        uses that dictionary for subsetting

    Parameters
    ----------
    data : Dictionary
        Dictionary containing all datasets. Dataset names = keys, datasets = 
        values. Can be access by data[key].
    key : string
        Name of dataset to subset from.
    criteria : kwargs
        Dataframe columns and their respective values you want to use for 
        subsetting a dataframe. Synt

    Returns
    -------
    subset : Dataframe
        Filtered subset of your input dataframe

    """
    # Make mask formatted ti query syntax
    mask = ' and '.join(["{} == '{}'".format(k,v) for k,v in criteria.items()])   
    
    # make subset
    subset = data[key].copy(deep=True).query(mask)

    return subset

# Function for dropping filename from dataframe
def filename_removal_func(df):
    """Removes 'filename' column from dataframes ...not really needed"""
    df = df.drop('filename', axis=1)
    return df



def associate_codes(df_in: 'pd.DataFrame',
                    df_codes: 'pd.DataFrame' = None,
                    ind_code_col: str = 'naaringskode',
                    nr_code_col: str = 'nr_naaring',
                    prod_name_col: str = 'produkt_tekst',
                    prod_name: str = None):
    """
    Associate two sets of codes in two different dataframes with each other,
    map to new column in output_dataframe.

    TLDR:
        Creates associations between codes stored in two columns in a df,
        reads column in a different dataframe containing one set of codes,
        then creates new column with corresponding codes.

    Parameters
    ----------
    df_in : pd.DataFrame
        Dataframe containing data and set of codes (NACE codes for example)
        you want to map new codes to (NR-codes).
    df_codes : pd.DataFrame
       Dataframe containing correspondences between old codes (NACE) and new
       codes (NR-codes). The default is data['omkoding_nr'].
    ind_code_col : str
        Dataframe column in df_in containing codes to use map correspondences
        on. The default is 'naaringskode'.
    nr_code_col : str
        Dataframe column containing codes you want to map to new column in
        df_in. The default is 'nr_naaring'.
    prod_name_col : str
        Dataframe column in df_in containing codes you want to use for mapping.
        The default is 'produkt_tekst'.
    prod_name : str
        Product name you want to use for subsetting correspondence df. Used for
        filtering the correct subset of mappings for a product. I.e. If the mapping
        is done for Jetparafin, prod-name = Jetparafin. NB: Is case sensitive.
        The default is None.

    Returns
    -------
    df : pd.DataFrame
        Output dataframe containing source data and new column w/ new codes

    """

    # Extract working data and copy
    df = df_in.copy(deep=True)
    coding_subset = df_codes.copy(deep=True)

    # Subset codes for given product according to substring
    mask = coding_subset[prod_name_col].str.contains(prod_name)

    # Apply subsetting
    coding_subset = coding_subset.loc[mask, [ind_code_col, nr_code_col]]

    # Associate product-code in output dataframe  w/ nr-code & add as new column
    d = dict(zip(coding_subset[ind_code_col], coding_subset[nr_code_col]))
    df[nr_code_col] = df[ind_code_col].map(d)

    return df


@input_argument_none_eliminator
def column_tidy_func(df_in, 
                     keep_cols=None, 
                     avgift_col=None, 
                     fillna_avgift=True):
    """Reshapes data frame to National Accounts T1/T2 table form"""
    
    # Input management
    keep_cols.append(avgift_col)
    df = df_in.copy(deep=True)     # Copy df to prevent mutability issues
    
    # Simple type check
    if isinstance(keep_cols, list) is False:
        raise TypeError('\n'.join([f'Wrong input type, keep_cols input must be a list.',
                        f'Current type of input is: {type(keep_cols)}']))
    
    # Tidy data
    df = (df
          .groupby(keep_cols, as_index=False)[keep_cols].sum(avgift_col) 
          .rename(columns={avgift_col : 'v_15',
                           'produkt_tekst' : 'produkt',
                           'nr_naaring': 'mottaker'})
          )
    
    # Provide names for new columns to generate
    additional_cols = ['v_11', 'v_12', 'v_16']
    
    # Creates additional columns with zeroes in them
    df_add_cols = pd.DataFrame(
        (np.zeros(shape=[len(df), 3])), columns=additional_cols)
    
    # Create ouput dataset
    df = pd.concat([df, df_add_cols], axis=1)
    
    # Define export column order
    column_order = ['produkt',
                    'ytart',
                    'mottaker',
                    'aar',
                    'v_11',
                    'v_12',
                    'v_15',
                    'v_16']
    
    # Set column order
    df = df[column_order]
        
    return df


def diff_maker(df, year_col='aar', total_fee_col='total_avgift_kroner', est_fee_col='est_avgift_kroner'):
    """
    Takes the difference per year between two dataframe columns.
    
    total_fee_col = X, est_fee_col = Y
    
    subset['diff'] = grouped sum per year of X - grouped sum per year of Y
    
    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing numerical data.
    year_col : str, optional
        Time column. The default is 'aar'.
    total_fee_col : TYPE, optional
        Column1. The default is 'total_avgift_kroner'.
    est_fee_col : TYPE, optional
        Column2. The default is 'est_avgift_kroner'.

    Returns
    -------
    subset['diff'] : pd.Series
        Series containing value difference

    """
    # Extracts subset copy
    subset = (df[[year_col, total_fee_col, est_fee_col, 'nr_naaring']]
              .copy(deep=True))

    # make estimated yearly fee series
    est_yearly_fee = (subset
                        .groupby(year_col)[est_fee_col]
                        .sum(est_fee_col))
    
    # Append estimated fees to df
    subset = (subset
              .drop(est_fee_col, axis=1)
              .merge(est_yearly_fee, on=year_col)
             )
    
    # Make diff column
    subset['diff'] = subset[total_fee_col] - subset[est_fee_col]

    return subset['diff']


# Velger ut produkter som skal omregnes til mineralolje
def rounding_error_dealer(df_in,
                          estimated_fee_col='est_avgift_kroner',
                          year_col='aar',
                          total_fee_col = 'total_avgift_kroner',
                          diff_func = diff_maker):
    """
    Takes the difference between a column of a known total and compares
    it with a column of disaggregated values of the total. If there is any
    sort of deviance (due to rounding error etc.) it will apply the difference
    to largest values in disaggregated values column.
    
    If there are multiple largest values the function will divide the 
    difference and spread them across the n largest observations.

    Parameters
    ----------
    df_in : pd.DataFrame
        Pandas dataframe containing data.
    estimated_fee_col : str
        Dataframe column containing estimated paid fees
    year_col : str, 
        Dataframe column containing time variable. The default is 'aar'.
    total_fee_col : str
        Known total sum. The default is 'total_avgift_kroner'.
    diff_func : Function
        Function used to calculate differences between known total and 
        estimated disaggregated values. The default is diff_maker.

    Yields
    ------
    df : pd.DataFrame
        Dataframe with corrected values

    """

    df = df_in.copy(deep=True)

    # Calculate diff column
    df['diff'] = df.pipe(diff_func,
                         year_col = year_col,
                         total_fee_col = total_fee_col,
                         est_fee_col = estimated_fee_col, 
                    )

    # Calculate total yearly fee
    total_yearly_fee = (df
                        .groupby(year_col)[estimated_fee_col]
                        .sum()
                        .rename('est_total'))

    # Indicate index subset for largest estimated fees in df
    idx = (df
           .groupby(year_col, sort=False)[estimated_fee_col]
           .transform(max) == df[estimated_fee_col])

    # Count amount of max values per year in max value subset
    # will yield >1 if there are identical max values within a year
    max_vals_per_year = (df.loc[idx]
                         .groupby(year_col)[estimated_fee_col]
                         .count().rename('n_max_obs'))

    df = df.merge(max_vals_per_year, on=year_col)

    # Attribute diff values to largest observation
    # divide by n largest if there are several
    factor = np.divide(df.loc[idx]['diff'], df.loc[idx]['n_max_obs'])
    corrected_fees = df.loc[idx][estimated_fee_col] + factor

    # Add results to dataset where applicable, then merge into column
    df['corr_fees'] = corrected_fees
    df[estimated_fee_col] = np.where(df['corr_fees'].notna(),
                                     df['corr_fees'],
                                     df[estimated_fee_col])


    return df


# Velger ut produkter som skal omregnes til mineralolje
def calculate_mineral_oil(df_in):
    """
    Calculates mineral oi usage in liters based on a set of products 
    and their usage multiplied by a constant

    Parameters
    ----------
    df_in : pd.DataFrame
        Dataframe containing energy accounts

    Returns
    -------
    df_out : pd.DataFrame
        Dataframe with energy usage of a subset of products in liters

    """

    subset = df_in.copy(deep=True)
    
    # Products to be conterted to liters
    min_oil = ['EP0467111',
         'EP04669',
         'EP046712',
         'EP046713',
         'EP04672',
         'EP0468']

    mask = subset["produktkode"].isin(min_oil)
    
    # Extract subset that will be converted into liters
    subset = subset[mask]

    # Lager ny variabel for Mengde_liter
    subset = subset.assign(mengde_liter='')

    # Oppretter variabelen omregningsfaktor for de ulike produktene
    subset.loc[subset['produktkode'] == 'EP0467111', 'omregningsfaktor'] = 1190476.19047619
    subset.loc[subset['produktkode'] == 'EP04669', 'omregningsfaktor'] = 1190476.19047619
    subset.loc[subset['produktkode'] == 'EP046712', 'omregningsfaktor'] = 1190000
    subset.loc[subset['produktkode'] == 'EP046713', 'omregningsfaktor'] = 1190476.19047619
    subset.loc[subset['produktkode'] == 'EP04672', 'omregningsfaktor'] = 1136363.63636364
    subset.loc[subset['produktkode'] == 'EP0468', 'omregningsfaktor'] = 1020408.16326531

    # Regner om til mineralolje i liter og setter produktkode og produkttekst til mineralolje
    subset['mengde_liter'] = (subset['mengde']*subset['omregningsfaktor'])
    subset['produktkode'] = 'mineralolje'
    subset['produkt_tekst'] = 'mineralolje (liter)'

    group = ['filename', 'aar', 'produktkode', 'produkt_tekst', 'naaringskode', 'naaring_tekst']

    # Aggregerer slik at det kun er en linje for hver næringskode for hver årgang
    subset = (subset.groupby(group)['mengde_liter'].sum()
                      .reset_index()
                      #.drop(columns=['mengde', 'omregningsfaktor'])
                      .rename(columns={'mengde_liter' : 'mengde'}))

    # Sletter variablene mengde og omregningsfaktor
    df_out = pd.concat([df_in[~mask], subset])

    return df_out
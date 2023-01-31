# Aggregation and disaggregation functions with ts_tools
 The ts_tools contains two functions for aggregation and disaggregation for timeseries. The behaviour is meant to mimic that of Fame. The file ts_agg_disagg.py contains these files.
 Two additional functions for demonstrations purposes are also present in ts_tools. These two are the df_generator function which generate n timeseries over a certain time interval with a given frequency (days, months etc.), and plotter which is a plotting function based on seaborn.

### Timeseries treatment functions in ts_tools
 #### disagg_func_stairs
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

 #### aggregation_func
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
    df : Dataframe.
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

### Demofunctions in ts_tools
Functions included for generation of fake data and plotting of results.

 #### df_generator
    Generates dataframe indexed by period with n timeseries that have trend
    and random noise. Can fill dataframe with a proportion of null values
    in random places if insert_null_values = True.
    
    Input arguments are positionally sensitive.

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

#### plotter
    Simple plotting function that takes a dataframe with timeseries as input, then plots result.
    The function requires a period or date-time like index in the input dataframe to work.

    Parameters
    ----------
    df : Dataframe 
        Pandas dataframe with datetime-like or period index

    Returns
    -------
    plot : plot object
        Finished plot with plotted timeseries.

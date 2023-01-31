# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 13:23:45 2022

@Author: Benedikt Goodman
Email: benedikt.goodman@ssb.no
"""

from functools import wraps

def input_argument_none_eliminator(func):
    """
    Checks for None in input arguments and keyword arguments in a given
    function.
    
    Works as a decorator function to add via pie syntax. I.e. function can
    be added wrapped around another function by invoking it with @. Example:
        
        @input_argument_none_eliminator
        def some_func(*args, **kwargs):
            do stuff
            return stuff

    Parameters
    ----------
    func : Function
        Any type of function.

    Raises
    ------
    ValueError
        Raises value error if None is detected as input argument or keyword 
        argument.

    Returns
    -------
    wrapper : Function
        The same function as was defined as input, but now with the added 
        check for None in function inputs.

    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Unpack inputs
        #positional_arguments = [*args]
        keyword_arguments_values = {**kwargs}.values()
        
        # Checks for none in function inputs (both arguments and keyword arguments)
        if None in keyword_arguments_values:
            raise ValueError('None present as argument or keyword argument in function. Please specify valid variables as inputs in function. \n',
                            'Hint: You might have forgotten to define an input. You can check inputs for functions in the documentation using ?function_name in a notebook')
        return func(*args, **kwargs)
    return wrapper

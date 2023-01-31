# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 09:28:27 2023

@author: Benedikt Goodman
@email: Benedikt.Goodman@ssb.no
"""

def check_listinput(dataset_list: list, list_name: str):
    """
    Type-check function to check if a variable is a list which is non-empty 
    and contains strings, floats and ints. Raises errors otherwise.

    Parameters
    ----------
    dataset_list : list
        Input variable to typecheck.
    list_name : str
        Name of variable to check. Used to improve program feedback for user.

    Returns
    -------
    None.

    """
    
    if isinstance(dataset_list, list) is False:
        raise TypeError(f'{list_name} is not a list. \n',
                       'Hint: DataPreparer(path, {list_name}=["item_1", "item_2"])')
        
    elif len(dataset_list) <= 0:
        raise ValueError(f'No items in {list_name}. Please sepcify which datasets to import in a list.')
    
    # Triggers if content of dataset_list is not str, int or float
    elif False in [isinstance(item, (str, int, float)) is True for item in dataset_list]:
        raise TypeError(f'{list_name} must only contain ints, str or float')
        
    else:
        return

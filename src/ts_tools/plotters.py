# -*- coding: utf-8 -*-
"""
Created on Tue Dec 20 11:39:02 2022

@author: Benedikt Goodman
@email: benedikt.goodman@ssb.no
"""

def plotter(df, x_label='Dato', y_label='MillNOK'):
    sns.set(rc = {'figure.figsize':(17,8)})

    dfc = df.copy()

    if dfc.index.dtype != 'datetime64[ns]':
        try: 
            dfc.index = dfc.index.astype(str)
            dfc.index = pd.to_datetime(dfc.index)

        except IndexError:
            raise IndexError('Index not convertible to datetime')

    # Lineplot maker
    sns.lineplot(data=dfc)
    sns.set_theme(style='darkgrid')
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.legend(labels=dfc.columns)

    return plt.show()
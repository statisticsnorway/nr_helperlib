# nr_helperlib



Created by: Benedikt Goodman <benedikt.goodman@ssb.no>

---

## What is this repo?
Helper functions for treatment of time series and more. Still under development.

Includes tools for aggregation and disaggregation of time series based on FAME's functionalities for timeseries. It also contains a lookup-function that yields product-, recipient and industry codes used in national account system.

For a short intro on functionalities and required libraries for operation of functions, see the readme files inside each function folder.

Currently these functions work best in the administrative zone, but the plan is to better integrate them with the production zone in the future so it can be connected to the production zone and dapla.

## Where are things located
All functions and classes reside in the src folder. Currently the repo features:
- data_import_tools which is a selection of elements that simplify import of data
- ts_tools which is will one day do many of the things FAME does for us today.
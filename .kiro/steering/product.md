# PyFIA Product Overview

PyFIA is a high-performance Python library that implements the R rFIA package functionality for analyzing USDA Forest Inventory and Analysis (FIA) data. It provides exact statistical compatibility with rFIA while leveraging modern Python data science tools.

## Core Purpose
- **Forest Analytics**: Programmatic API for working with national forest inventory datasets
- **Statistical Compatibility**: Maintains exact statistical compatibility with the popular rFIA R package
- **Performance**: Leverages DuckDB and Polars for efficient processing of large-scale data

## Key Features
- **Core rFIA Functions**: Trees per acre (`tpa()`), biomass (`biomass()`), volume (`volume()`), forest area (`area()`), mortality (`mortality()`), growth (`growMort()`)
- **Statistical Methods**: Design-based estimation, post-stratified estimation, temporal estimation methods (TI, annual, SMA, LMA, EMA)
- **Data Processing**: DuckDB backend for large-scale queries, Polars DataFrames for in-memory operations
- **EVALID Management**: Automatic handling of FIA's evaluation-based data structure

## Target Users
- Forest researchers and analysts
- Government agencies working with FIA data
- Academic institutions studying forest inventory
- Environmental consultants and organizations

## Design Philosophy
- **Drop-in Replacement**: Designed as a Python replacement for rFIA with identical statistical outputs
- **Performance First**: 10-100x faster for large-scale queries, 2-5x faster for in-memory operations
- **Statistical Accuracy**: Exact statistical accuracy compared to rFIA following Bechtold & Patterson (2005) methods
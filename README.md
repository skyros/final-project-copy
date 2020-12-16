[![Build Status](https://travis-ci.com/skyros/2020fa-final-project-skyros.svg?token=18E2pJv6upgk4axV7LFv&branch=master)](https://travis-ci.com/skyros/2020fa-final-project-skyros)
[![Maintainability](https://api.codeclimate.com/v1/badges/d4e37982a111e966754c/maintainability)](https://codeclimate.com/repos/5fc79c63d7c28d43c7000ccb/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/d4e37982a111e966754c/test_coverage)](https://codeclimate.com/repos/5fc79c63d7c28d43c7000ccb/test_coverage)

# Final Project

Geospatial Data Visualization of Covid-19 Data Using GeoPandas, Luigi, Dask, and Bokeh

## Installation

Use [pipenv](https://pypi.org/project/pipenv/) to install the project dependencies.

```bash
pipenv install
```

## Usage

```bash
pipenv run python -m final_project [-h] [-i] [-o]
```

## Project Requirements

### Packages

- CSCI-Utils
- Dask[dataframe]
- GeoPandas[GeoDataFrame]
- Luigi
- FastParquet
- Bokeh
- Requests
- Boto3

### Dev-Packages

- Pytest
- Pytest-cov
- Shapely

## Project Description

### Summary

For my CSCI E-29 Final Project, I chose to build a Luigi Pipeline to visualize Covid-19 geographically. The pipeline uses API calls to The Covid Tracking Project and The US Census Bureau as data sources and as such uses up to date datasets. The data is then condensed, cleaned, merged, and eventually visualized with Bokeh to produce an interactive HTML file similar to what is seen below.

##TODO [Insert Image]

#### *A Note About The Shapefile Data:*

As there is no API for shapefiles at the US Census Bureau, the files had to be downloaded manually and uploaded to S3. While my project was designed to initialize the shapefile pipeline at S3, I have provided the files (and subsequently designed an intermediary task to hit them the pipeline) so that my project can be run without needing my S3 credentials.

## Advanced Python Highlights

### Expanding Luigi Composition

The construction of the Luigi tasks uses the pset_ 5 composed versions of `requires`, `requirement`, and `output`. However, it extends the functionality of these classes by adding additional `target` classes allowing for read and/or write functionality of GeoPandas' `GeoDataFrame` and Bokeh's `figure`. The implementation of these can be found in [utils.py](final_project/utils/utils.py).

#### Some things to note here:

I needed a way to handle Shapefile Targets both as LocalTagets or S3Targets. The read and write methods, however, remain the same between these two classes. I somewhat follwed the method from pset_5 with `CSV_Target` and `ParquetTarget`, which inherit from a base class. However, `LocalShapeFileTarget` and `S3ShapeFileTarget` have their read and write methods established in the base class, then use `LocalTarget` and `S3Target` as a mixin using the new write methods from the base class to extend the respective Luigi targets.

### Testing

The [test file](test_pset.py) for this project is rather extensive and accomplishes some pretty cool things with regards to testing with API calls and testing Luigi Tasks. The setup was rather tedious for some of the tests, which contributed for a little bit of verbosity in places. However, I think the project demonstrates a good overall method for testing Luigi tasks.

#### Testing Luigi

My general philosophy for testing Luigi Tasks has become to focus entirely on the `run()` method as this is where most of the logic of the Task is. The `requirement` and `output` portions tend to be farily boilerplate and usually get tested implicitly. As such, the general process for each task is as follows:

1. Generate test input and expected output.
2. Save the test input in a temporary folder.
3. Create a Luigi `ExternalTask` with the saved folder as the output target in the format the task we are testing expects.
4. Create a Test Luigi Task that inherits from the Task we are testing but requires the `ExternalTask` we just wrote and outputs to a temporary folder.
5. Open the output file and compare it to the expected output.

Im viewing my tests, you will see most of my tests follow this convention.

#### Testing API Calls

I think on of the biggest breakthroughs I had while putting this project together was finally getting a solid understanding of UnitTest `mock.patch` and the `@patch` decorator. This was used for `test_pop_data` and `test_covid_data` in the test suite. In these tests, I use `@patch` to intercept a `reuqests.get` call. With both API calls, the format was different. In one, I recieved a csv file, in the other I recieved a json array. Within each of these tests, I create a `MockedAPIResponse` class with a method or a property that mocks the behavior of the API for its respective Task, contaning fake data that was written in the test. That `MockedAPIResponse` is then sent as the return value of the intercepted `requests.get` call and it's method/property can then be accessed, just as it would be from the actual API.

## Data
- [The Covid Tracking Project at The Atlantic](https://covidtracking.com/) - Covid-19 Data
- [The United States Census Bureau](https://www.census.gov/) - US State Shapefiles/Population Data

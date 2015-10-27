# Forecast Specification Files Folder

This folder and every subfolder here will be searched for forecast specification files with .yaml extension on runtime.

## Forecast Specification File Structure

A forecast specification file is a plain text YAML file that details the different simulations that the system should create and run.
More specifically, a forecast is a group of simulations
The forecast specification tree has the following fields:

* **name**: the forecast name (*optional*).
* **crop_type**: the crop type code (eg. SB, MZ, etc). This field is used to identify different crop forecasts in the results database, therefore it can be user-defined to whatever is needed.
* **crop_template**: defines the filename of the pSIMS experiment template inside the `data/templates` folder.
* **configuration**: a key-value set that extends the system config to define specific configuration fot this forecast.
    * **weather_series**: indicates the type of weather serie that the derived simulations should use (combined or historic).
    * **max_parallelism**: defines the maximum number of tasks that may be ran in parallel for this forecast.
* **forecast_date**: a date (or a list of dates if you wan't to define multiple forecasts) in which the forecast takes place. If the forecast is configured to use historic weather series, this field is ignored. 


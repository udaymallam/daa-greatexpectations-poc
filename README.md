# daa-greatexpectations-poc
# Geat Expectations Automation Test Framework

## Installation 
* Install python (select env and pip while installation)
* Setup and activate virtual environment using => ``` virtualenv <name> and Scripts/activate ```
* Install dependencies using => ``` pip install -r requirements.txt ```(if new dependencies added then update requirements.txt => ``` pip freeze > requirements.txt ```)
* Initiate great_expectations using => ``` great_expectations init ```
* [Great expectations installation further info](https://docs.greatexpectations.io/docs/guides/setup/installation/local)

## Configure connection 
* Connection types: (Inferred/ Configurred/ Runtime)

## Setup Expectations and validate expectations
* Ways to setup expectations
	* Using profiler
	* Manualy using out of the box expectations 
	(Note: If Expectations are being created using profiler and manually, manual expectations should be created after profiler expectations). More info on out of box expectations @ https://greatexpectations.io/expectations
	* Custom expectations
* Save expecations and the validation results against the expectations as a bench mark

## Revalidate the expectations against the saved validation results (Check points)
* Checkpoints can be configured on the same data or similar data in different envrionments which needs to be validated

## Results/Report
* Reports pointed to reports folder in great_expaectations.yml file

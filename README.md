# All state classification Model

This project contains codebase for data preparation and summerization before passing it to the machine learning model for classifcaion.

## Requirement
1. Python
2. Pandas
3. Numpy
4. tqdm
5. sci-kit learn

## Installation
* open your terminal & cd into your project directory
* run `git clone https://github.com/Databreedteam/all-state-project.git` or download the zip from this location 	`https://github.com/Databreedteam/all-state-project`
* cd into the `all-state-project` cloned directory
* run `pip install -r requirements.txt` from your terminal to install the dependencies
* finally, run `python data_wrangler.py` and wait for the progress bar to finish
* go to the result folder, you will see the computed summary of the train csv file.


## Class
To initialize this class, use the code below

`Data =  DataWrangler()`

## Method  

### 1. convertToDataframe()
This method should be called immediately after Initializing the class.

`full_df, quote_df, purchased_df = Data.convertToDataframe()`

It accepts the `train.csv` as input then return a dataframe as output.

### 2. splitDataFrameToSections()
`full_df , quote_df, purchase_df = Data.splitDataFrameToSections()`

This method recive dataframe as input then return 3 different dataframes types as output.. which are :

1. `full_df` variable contains Full dataframe containing all quotes and purchased insurance.
2. `quote_df` variable contains dataframe of all quotes insurance only.
3. `purchased_df` variable contains dataframe of all purchased insurance only.


### 3. prepareDataForOperation()
`df = Data.prepareDataForOperation(quote_df)`

This method accepts dataframe and performs operation such as filling or removing missing variables on the colums and return the updated dataframe.


### 4. transformstateToCensusRegion()
`df = Data.transformstateToCensusRegion(df)`

This method takes in a datafram then convert the state colum into US censorship region by grouping them into `(mid_west, north_east, south, west)` then return an updated dataframe.

### 5. transformAgeToCategorical()
`df = Data.transformAgeToCategorical(df)`

This methods takes in a dataframe the transform the age from numerical data to categorical data by diving them into (LOW, MED, HIGH) then return the updated dataframe.

### 6. performWeightedAverageOnAG()
`df = Data.performWeightedAverageOnAG(df)`

This method takes in a dataframe and compute the weighted average on all the insurance policy features.. i.e colum A-G, then return the updated dataframe

### 7. transformTimeOfTheDay()
`df = Data.transformTimeOfTheDay(df)`

This method takes in a dataframe then transform the time of the quote to a categorical values `(MORNING, AFTERNOON, NIGHT)` then return updated dataframe.

### 8. performWeightedAverageOntTime()
`df = Data.performWeightedAverageOntTime(df)`

This methods takes in a dataframe and computes the weighted average on the time of the quotes and return an updated dataframe.

### 9. summerizeQuote()
`df = Data.summerizeQuote(df)`

This method takes in a dataframe and summerize duplicates entry quotes for customers with same `customer_ID` then return the updated dataframe.

It will also drop some colum that wont be needed.

### 10. saveToDirectory()'
`Data.saveToDirectory(df, "summerized_result.csv")`

This method can be called at anypoint in your implementation of this code to save the dataframe.

It takes in a dataframe and also a filename ending with a .csv extension and save the result of the dataframe as csv file into the result folder.
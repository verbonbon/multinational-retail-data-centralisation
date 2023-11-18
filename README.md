# Project Title: Multinational Retail Data Centralisation

#    Table of Contents
1. [Background](#background)
2. [How to Install](#install)
3. [File structure](#file_structure) 
4. [License information](#license) 
5. [What I have learned](#learned) 


##   Background of the project <a name="background"></a>
This is a project I completed for my data engineering immersive training,<br/> 
at [AI Core](https://www.theaicore.com/).

This aim is to produce a centralized database to run data processing pipelines,<br/>
for a multinational company that sells various goods across the globe.<br/>
<br/>

**Challenged faced by the team:**<br/>
The sales data is spread across many different data sources (e.g., AWS S3 bucket, API)<br/>
and data format (e.g., csv file, json, pdf). Hence, it is difficult to access and analyze<br/>
the data by current members of the team.<br/>
<br/>

**My proposed solution:**<br/>
Create a centralised location for all data in a database,<br/>
where business queries can be performed.<br/>
Demonstrations of the queries can be found in the SQL_business_query.sql file<br/>
<br/>
<br/>

##    How to install? <a name="install"></a>
Clone this project <https://github.com/verbonbon/multinational-retail-data-centralisation>
 <br/> 
Run the following on your terminal to extract, clean, and create the database: <br/> 
python3 main.py <br/>
<br/>
Run the following on your PostgreSQL platform to develop the database schema: <br/>
SQL_database_schema.sql<br/>
<br/>
Run the following on your PostgreSQL platform to perform the business queries: <br/>
SQL_business_query.sql<br/>
<br/>
<br/>

##    File structure of the project <a name="file_structure"></a>
### The python files
There are four python files. The first three has a class in each, to perform specific tasks:<br/>
1. data_extraction.py
    - The class is a utility class, called DataExtractor,<br\>
     with methods that extract different data type from the following data sources:<br\>
        - dataframe from SQL database [with information about users]
        - pdf file from AWS cloud [with information about card payments]
        - json file through API [with information about stores]
        - csv file from AWS S3 bucket [with information about products]
        - json file from AWS S3 [with information about sales date]
    - If this run is run on its own, it will produce the first 5 lines of<br\>
    data from each dataframe<br\>
<br\>

2. data_cleaning.py
    - This class is called DataCleaning
    - It has methods to clean data from each of the data sources
    - Null entries are null entries into a format pandas can recognise (np.nan)
    - Dates are parsed into date time format
    - Irregular expressions and invalid entries are removed
    - Units of measurement (e.g., weight) are standardized<br\>
    - If this run is run on its own, it will produce the first 5 lines of<br\>
    data from each cleaned dataframe<br\> 
<br\>

3. database_utils.py
    - This class is called DatabaseConnector
    - It has methods that connect with and upload data to the database.<br/>
<br/>

4. main.py
    - This python files calls and initiate all the classes and methods necessary<br\>
    to run the data processing pipeline. 
    - This is the file to run to extract, clean, and upload the dataframes in one go<br/>
<br/>

### The SQL files
1. The SQL_database_schema.sql file creates the schema of the database, <br\>
to ensure that the columns are of the correct data types.<br\>
The primary keys and foreign keys are also defined.  
2. The SQL_business_query.sql file answers 9 business queres, <br\>
by analyzing individuals dataframe, or by joining dataframes to <br\>
produce insights for business operations and stakesholders.<br\>
<br\>
<br\>

##    License information <a name="license"></a>
MIT

##    What I have learned <a name="learned"></a>
The key lesson is that data cleaning constituted the bulk of the data processing work.<br/>
Doing a thorough inspection and data cleaning early on<br/>
will save time on the SQL database schema creation in later stages. <br/>
There were multiple times that I had to go back to resolve data cleaning issues,<br/>
because the dataframes would not link up across the dataframes.<br/>

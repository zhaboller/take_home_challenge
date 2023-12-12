# Scala Data Challenge
## Objective
In this repository, I present my solution to a challenging data analysis task. The purpose of this project is to analyze and extract insights from advertising data, using Scala to perform tasks like data cleaning, aggregation, and time series analysis, demonstrating my technical proficiency in data processing and software engineering practices.

## Task Overview
The challenge encompasses the following tasks using a provided Dataset.csv file containing adtech-related data:

- Data Extraction: 

    - Reading and parsing the data using Scala.
- Data Cleaning and Processing: 

    - Cleaning and transforming the dataset for analysis.
    - Implementing a configuration file for parameters like file paths and data schema.   
- Data Aggregation:

    - Calculating total impressions by site and ad type.
    - Computing average revenue per advertiser.
    - Analyzing revenue share by monetization channel.
    - Producing summaries or visualizations from the aggregated data.
- Time Series Analysis

    - Conducting a time series analysis on a chosen metric over the date field.
    - Presenting findings in a clear and insightful manner.
- Unit Testing:

    - Writing unit tests for each component using a Scala testing framework such as ScalaTest.



## Dataset Description
The dataset provided in this project contains detailed records of adtech data, which has been used to analyze advertising performance metrics. Each row in the dataset represents an ad event with attributes captured at a specific timestamp. Here are the attributes included in the dataset:

- date: The timestamp of the ad event.
- site_id: The unique identifier for the website where the ad was displayed.
- ad_type_id: The identifier for the type of ad.
- geo_id: The geographical location ID where the ad was served.
- device_category_id: The category of the device on which the ad was displayed.
- advertiser_id: The unique identifier for the advertiser.
- order_id: The order ID associated with the ad display.
- line_item_type_id: The type ID for the line item in ad campaigns.
- os_id: The operating system ID on which the ad was displayed.
- integration_type_id: The ID for the type of integration used for serving the ad.
- monetization_channel_id: The channel through which revenue is generated.
- ad_unit_id: The identifier for the specific ad unit.
- total_impressions: The total number of times the ad was displayed.
- total_revenue: The total revenue generated from the ad.
- viewable_impressions: The number of times the ad was viewable.
- measurable_impressions: The number of times the ad impressions were measurable.
- revenue_share_percent: The percentage share of the revenue.


## Running Instructions
This project is designed to run in an environment with the following dependencies:

- Java version: 1.8.0_392
- Apache Spark version: 3.5.0
- Scala version: 2.12.18
- SBT version: 1.9.7

Clone the repository and navigate to the project directory:
```bash 
git clone https://github.com/zhaboller/take_home_challenge.git
```
Update the configuration file `src/main/resources/application.conf`: 
- paths.dataset: The file path to the dataset (Dataset.csv)
- schema.fields: The data schema for the dataset (Dataset.csv)

Compile and execute the code
```bash 
sbt compile
sbt run
```
Use the following code to run test cases
```bash 
sbt test
```


Here are some dependencies used:
- spark-core and spark-sql are essential for data processing and analysis tasks.
- spark-mllib is included for machine learning tasks, which might be necessary for advanced data analysis such as time series analysis.
- scalatest is for writing and running unit tests, ensuring the robustness of your code.
- plotly-scala is a plotting library to visualize data, which is useful for generating insights from the data analysis.

## Challenge and solution
### Challenge 1: Environment Version Compatibility
- Challenge: Inconsistent versions between runtime environments such as Spark, Scala, and Java can lead to incompatibility issues, where the application may not run or certain functions may not execute as expected.
- Solution: To address this, I specified the versions of each technology in the build.sbt and project configuration files. This ensures that the project will be built and run using compatible versions, preventing version mismatch issues. For future-proofing, a Docker container can be used to set up the environment, guaranteeing the same setup across different machines.

### Challenge 2: Limited Scala Visualization Tools
- Challenge: The availability of visualization libraries for Scala is limited, and many such tools have not been updated for several years, leading to outdated support and potential security vulnerabilities.
- Solution: To overcome this, I integrated Plotly Scala, a cross-language library that is mature and stable. Plotly Scala provides a wide range of visualization options that are both modern and compatible with our data analysis needs. Additionally, for more complex visualization needs or when dealing with large datasets, I recommend sampling the data and then using Python's visualization libraries.

## Contributer
Gaohua Zheng

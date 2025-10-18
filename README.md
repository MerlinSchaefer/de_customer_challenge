# The task

## 1. Draft concept for one or more pipelines

We work with various customers, each with different ERP systems. We extract sales, delivery, article, and store master data from these ERP systems via the respective interfaces on a daily basis and store it in a cloud. Some example dataset for 3 customer are provided.

Outline the structure of one or more pipelines with the following objectives:   

One or more tables are provided, which can

- be used for the development of machine learning models. The model requires the sales quantity in pieces, sold out yes/no for each product, store, and day. 

-  be displayed in an app for presentation and evaluation. For this purpose, the sales quantity in units, sold out yes/no, return quantity, and delivery quantity are required for each product, store, and day. In addition, store and product information should also be displayed. 



**Overview of required data:**

| Column           | Type                  | Description                                                                 |
|------------------|-----------------------|-----------------------------------------------------------------------------|
| id_product       | INT, not null         | Unique identifier for the product                                           |
| id_store         | INT, not null         | Unique identifier for the store                                             |
| target_date      | DATE, not null        | Calendar date for the record                                                |
| sales_qty        | DECIMAL(10,2), DEFAULT 0 | Quantity of items sold on target_date                                   |
| return_qty       | DECIMAL(10,2), DEFAULT 0 | Quantity of items returned on target_date                               |
| delivery_qty     | DECIMAL(10,2), DEFAULT 0 | Quantity of items delivered to the store on target_date                  |
| stockout         | BOOLEAN, DEFAULT FALSE   | Indicates if product was sold out on the target date (1 = yes, 2 = no)   |
| price            | DECIMAL(10,2), NULL      | Price of the product on target_date                                       |
| product_name     | VARCHAR(255), NOT NULL   | Name of the product                                                       |
| number_product   | INT, NOT NULL            | Customer’s product number                                                 |
| moq              | INT, DEFAULT 0           | Minimum order quantity for the product                                    |
| number_store     | INT, NOT NULL            | Customer’s store number                                                   |
| store_name       | VARCHAR(255), NOT NULL   | Name of the store                                                         |
| store_address    | VARCHAR(255), NULL       | Physical address of the store (street – PLZ – City)                       |

## 2. Implementation of the concept

Implement your drafted ETL-pipeline concept using the sample data provided via mail.

A small Kedro project is provided in the folder coding-challenge as a starting point. You are free to choose kedro as a framework or another framework if you are more comfortable with it.

Expected results:

- The pipeline should transform the raw data into standardized tables that cover all fields required in Task 1 (see also **Overview of required data**).

- The pipeline should produce outputs that allow successful execution of the notebook coding-challenge/notebooks/Example_plot.ipynb.

We would like you to do a live execution of the pipeline for all customers and the creation of the plots during the second interview. 


# Installation guide

create python environment using python 3.9.23.

e.g.
````
conda create --name CodingChallenge python=3.9
````
install all requirements 

```
pip install -r <path-to-folder>src/requirements.txt
```

set your folder path in 

```
 conf/1001_customer/globals
```

while beeing inside of folder coding-challenge the following command starts a test pipeline:

```
kedro run --env 1001_customer --pipeline etl_example
```

## Need help?

[Find out more about configuration from the Kedro documentation](https://docs.kedro.org/en/stable/kedro_project_setup/configuration.html).

For documentation of the sample data, see the Readme in the data folder.


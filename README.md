# Walmart-Data-Ingestion-In-BigQuery-Project 
<h2>____________________________________________________________________________________________________________________________________________________</h2>

  <br>
  <br>
   

## PROJECT OVERVIEW

This project is designed to automate the ingestion of Walmart's sales and merchant data into Google BigQuery using Google Cloud Storage (GCS) and Airflow. The data arrives in JSON format, and the goal is to update BigQuery tables to maintain the latest data for analytics and reporting.

 <br>




### PROJECT EXPLANATION :-  

  #### Datasets

   ###### We assume two datasets:
  
   <h3>Merchant</h3> : 
   This dataset is updated periodically (daily or weekly) with a full snapshot of the latest merchant data. For instance, if the first merchant file received in January contained 50–60 merchants, a subsequent file might contain updated data for those merchants, along with new records. This latest file always replaces any previous merchant data.
       <br>
   -  <h3>Sales</h3> :  Sales data arrives daily, recording Walmart’s sales transactions..
   
      

   ## Data Processing Steps:

  <h3>1. Read Data from GCS:</h3>
  Both merchant and sales data are stored as JSON files in a GCS bucket.

<h3>  2.  Merchant Data Loading:</h3>

 Since the merchant data file contains a complete snapshot, we perform a truncate-and-load operation, where the existing data is overwritten by the new data.
<h3>3. Sales Data Staging:</h3>

 Sales data is ingested daily and loaded into a staging table in BigQuery. The staging table is overwritten each time to maintain only the latest data.
<h3>4. Data Joining and Transformation:</h3>

 A join operation is performed between the merchant table and the sales_stage table to create a final_sales table, providing enriched sales information with merchant details.
<h3>5. Update Target Table:</h3>

 An upsert (update and insert) operation is performed on the final_sales table, ensuring it holds both the latest and historical sales records.
      
     

  
























<br>
<br>
<br>

## ARCHITECTURE DIAGRAM :-

![Project Architecture](Walmart_p1.drawio.png)  










<br>
<br>
<br>

## TECHNOLOGY USED :-
*  Programing Language :- Python
  
*  Scripting Language  :- SQL
  
* ### Google Cloud Platform
 
    - #### BigQuery
      
    -  #### Cloud Storage(GCS)
      
    - #### AirFlow for orchestration










<br>
<br>
<br>

## Dataset Used  :-
### Merchant Dataset link -
- https://github.com/aadarsh786/Walmart-Data-Ingestion-In-BigQuery-Project/blob/main/merchants_1.json
- https://github.com/aadarsh786/Walmart-Data-Ingestion-In-BigQuery-Project/blob/main/merchants_2.json

<br>

### Sales Dataset link -
- https://github.com/aadarsh786/Walmart-Data-Ingestion-In-BigQuery-Project/blob/main/walmart_sales_1.json
- https://github.com/aadarsh786/Walmart-Data-Ingestion-In-BigQuery-Project/blob/main/walmart_sales_2.json








<br>
<br>
<br>

## Python Script File  :-
[AirFlow-BigQuery-Dag-File](practicepu.py)
  <br>
  <br>
## Resulted output :-











# Walmart-Data-Ingestion-In-BigQuery-Project 

  <br>
  <br>
  <br>
   







## EXPLANATION OF PROJECT :-  

+  Lets assume we are getting the data in json form now imagine we have two datasets.
  
   - <h3>Merchant</h3> : This data that we are getting every day or every week basically it contains latest data everytime.
     - Eg:-Lets say that first merchant file we got in january it was only having the data of 50-60 merchant.
      -  after one or two months we got this same file again with data of lets say 100 merchant which is fresh data
         basically the data we have received in recent times it contains the previous data and recent data ie:-merchants_1.
       <br>
   -  <h3>Sales</h3> :  This data we are getting on daily basis that is walmart_sales_1.
   
       <br>

       ### PROCESS:

      *  It will read data from gcs bucket.
      *  Everytime we are going to get a files for merchant that contain full latest data .
      *  We will overwrite the merchants data which already exists by new one so we are performing full truncate and then load operation.
      *  Sales data we are getting on daily basis  this data will store in stage table in BigQuery.
      *  same overwrite operation will perform on stage table.
      *  Performing join operation between merchant and sales_stage table will get finalsales table.
      *  Performing update and insert ooperation on final_sales table and load it into the target table as final output.
         
      
     

  
























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
  
*  Google Cloud Platform
 
    - BigQuery
      
    - Cloud Storage
      
    - AirFlow










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











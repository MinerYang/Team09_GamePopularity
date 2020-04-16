# GamePopularityPredictionSystem
## Team Members
- Jiaao Yu
- Miner Yang
- Runjie Li


> ## Description
_In order to usefully apply knowledge of Scala and Big data to a problem with practical significance, basically we plan to develop a reactive prediction system about new-released game popularity to help people, including new players, game companies etc.to predict how popular it would be based on poplarity levels._
- Client: get a prediction result including score(1-5) and popularity levels. 
- Adminstrator: in charge of system updating(model refreshing).
- popularrity levels
  - **Overhwelmingly Positive**: (95%-99%) 5 stars
  - **Very Positive**: (90%-95%) 4 stars
  - **Positive**: (80%-90%) 3 stars
  - **Mostly Positive**: (70%-80%) 2 stars
  - **Negtive**: (0%-70%) 1 stars
- Video: https://youtu.be/7T1Z-EI4PRU
- Presentaion: https://docs.google.com/presentation/d/1I0_m9uM0iG6S9PMNO0xilSdzMWVaPFJFlovXNQxXB54/edit?usp=sharing
<p>&nbsp;</p>

> ## Dataset
-	Source https://www.kaggle.com/nikdavis/steam-store-games
-	contains 27050 records about games information on steam platform
<p>&nbsp;</p>

> ## Model Training
  - Source: /RatingModelTraing
  - Main job: Data Cleaning, FeatureEngineering, PipelineBuild, ModelTraing, Unit test, Simple user test
  - tools: scala, spark
  - required doccuments: steam.csv
  ### run model Training app
  ```
  run on any IDE with scala and spark configuration 
  ```
<p>&nbsp;</p>

> ## Back End
  - Source: /RatingServer2.0
  - Main job: REST web back end, using pipeline to do real-time prediction, real-time model training
  - tools: spark, Pyspark, python, flask
  - required doccuments: my_pipeline, best_model, cleandata.parquet
  ### run the back app
  ```
  run on any python notebook 
  ```
  ### connection test 
  ```
  curl http://localhost:5000/predict 
  curl http://localhost:5000/train
  ```
<p>&nbsp;</p>
  
> ## Front End
  - Source: /RatingWebsite2.0
  - Main job: REST web front end, client to get prediction result, adminstrator to manage prediction model
  - tools: node.js, vue
  ### run the front app
  ```
  cd dist/spa
  python3 -m http.server --bind localhost 8080
  open  http://localhost:8080
  ```
<p>&nbsp;</p>





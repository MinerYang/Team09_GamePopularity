# GamePopularityPredictionSystem
## Team Members
- Jiaao Yu
- Miner Yang
- Runjie Li

> ## Description
In order to usefully apply knowledge of Scala and Big data to a problem with practical significance, basically we plan to develop a reactive prediction system about new-released game popularity to help people(including new players, game companies etc.) to predict whether this game would popular or not as well as how popular it would be.
- Client can get a prediction result including score(1-5) and popularity levels. 
- Adminstrator is in charge of system updating(model refreshing).
- popularrity levels
  - **Overhwelmingly Positive**: (95%-99%) 5 stars
  - **Very Positive**: (90%-95%) 4 stars
  - **Positive**: (80%-90%) 3 stars
  - **Mostly Positive**: (70%-80%) 2 stars
  - **Negtive**: (0%-70%) 1 stars


> ## Dataset
-	Source https://www.kaggle.com/nikdavis/steam-store-games
-	contains 27050 records about games information on steam platform

> ## Model Training
  - Source: /RatingModelTraing
  - Main job: Data Cleaning, FeatureEngineering, PipelineBuild, ModelTraing, Unit test, Simple user test
  - tools: scala, spark
  - required doccuments: steam.csv

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





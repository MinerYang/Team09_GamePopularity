## what kind of data structure should be used to fit our ML and Web visualization
Since when we do ML like KNN, it needs to be in a table format. Additionally, the implementation assumes that all columns contain numerical data. And the last column of our data has labels.

> ### About our project
[SteamStoreDataset](https://www.kaggle.com/nikdavis/steam-store-games#steam.csv)
1. Is **Ratings** == **Popularity** ?
2. If not, then how to calculate the popularity score? (should consider problems like how to validate the model accuracy in the future)
3. what kind of use cases specifically? (actor, actions, response)
    - admin, game publisher/developer, or other regular user?
    - what kind of results they will get by using our system?

> ### About ML
Reference
- [KNN](https://towardsdatascience.com/machine-learning-basics-with-the-k-nearest-neighbors-algorithm-6a6e71d01761)
- [NickBear_DataScience](https://github.com/nikbearbrown/INFO_6105)
- [NickBear_BigData](https://github.com/nikbearbrown/CSYE_7245)

1. classification or regression ?
    - classification: discrete value as its output (in our case would be rating 1-5)
    - regression :
2. supervised or unsupervised?
    - supervised: learn a function give a result of given labels
    - upsupervised: learn a function that will allow us to make predictions given some new unlabeled data
3. which columns would be used to feature extraction and ML job?
    - Maybe some correlation analisis sould be done
4. Algorthim exploring in spark/scala(MLlib)
    - which model training algorithm we plan to use? It basically depends on either classification or regression 
    - And how to implenment it in spark/scala (some one should explore this ahead)
5. Basic process
    - ~~Data Collect~~
    - **Data Cleaning** (data types, missing value, null value detecting and correcting )
    - **Feature Observing** (Correlation matrix and headmap maybe, more resaerch should be done) 
    - Model Training
    - Embed model into Web apllication (Flask maybe)

> ### More to be consider..
1. Database
2. Using more tool mentioned in class (akka,etc.) 
3. ...

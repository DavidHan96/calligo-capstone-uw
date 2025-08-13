# Calligo
# 📊 Calligo: A UW Capstone project

Calligo is a full-stack data pipeline that pulls macroeconomic time series from the [FRED API](https://fred.stlouisfed.org/), stores them in a s3 database as csv files, and analyzes them using forecasting models (VAR, Monte Carlo, XGBoost). The project is containerized with Docker and includes a Dash web app for interactive exploration.

---

## 🚀 Features

- 🔄 Automated data ingestion from FRED
- 📈 Forecasting models (VAR, Monte Carlo simulation, XGBoost classifier)
- 📊 Dash web app for visualizations
- ☁️ AWS-ready (S3, Glue-compatible)
- 🐳 Fully containerized with Docker

---

## 🛠️ Technologies

- Python, Dash, Plotly
- PostgreSQL (optional integration)
- Docker
- Pandas, Scikit-learn, statsmodels
- AWS S3 

---

## My Role & Contributions
- Built the data ingestion & preprocessing pipeline (FRED → cleaning → feature engineering)
- Implemented and tuned XGBoost + Monte Carlo workflows; ran/validated experiments
- Containerized the project with Docker for reproducible runs
- Authored documentation and demo notebooks; prepared final presentation assets

---  

## 📦 Setup 

### Fetching data
1. To fetch and store the data in your s3 instance, start by inserting your Fred API key from [FRED](https://fred.stlouisfed.org/docs/api/api_key.html) in the 12th line of FRED_crawler.py
2. Using your s3 keys, replace line 20 with your aws access key ID, 21 with your aws secret access key and your endpoint URL.
3. Make sure you have already created a bucket named fred in your s3 instance or rename line 23 with your preferred bucket.
4. (Optional) Change the series limit on line 17 if you do not want to have it go through all of them
5. To start fetching data, have your terminal in the /Data_Fetching folder and use these commands:
```bash
# 6.  Build the image (tagged “fred-crawler”)
docker build -t fred-crawler .

# 7.  Run the container
docker run fred-crawler
```
   
### Dash
1. To start the Dash app, have your terminal in the /Dash folder and use these commands:
```bash
# 2.  Build the image (tagged “dash”)
docker build -t dash .

# 3.  Run the container
docker run -p 8050:8050 dash
#            ^^^^ host : container
#            (use 8051:8050 if 8050 is busy)
```
4. Open any browser at http://localhost:8050

### Running the models
1. To run the models which aren't already loaded with the Dash app, simply use any app which lets you run python notebooks and load the files located in Calligo/models/dash_notebooks.
2. Make sure that you have your s3 instance with the data and change the first code block with your aws credentials
3. If you do not have the data loaded yet, or a gpu for the monte carlo, the original files in the Calligo/models/original folders contain models which run with data manually downloaded from [FRED](https://fred.stlouisfed.org/docs/api/api_key.html)

### Data fetching without s3
1. If you do not have access to s3, you can still upload the data in your local machine by having your terminal in Calligo/off_s3/local_db and use the following commands:
```bash
# 2.  Compose the image in the background
docker-compose up -d

# 3.  Run the SQL instance
docker exec -it fred_postgres psql -U fred_user -d fred_data
```
4. If you do not wish to use an postgres instance, have your terminal in Calligo/off_s3/local_csv and use the following commands:
```bash
# 5.  Build the image (tagged “fred-crawler”)
docker build -t fred-crawler .

# 6.  Run the container
docker run fred-crawler
```

---
## 🧑‍🎓 Presentation of the project
![Slide 1](slides/Slide1.png)
![Slide 2](slides/Slide2.png)
![Slide 3](slides/Slide3.png)
## 🗺️ Solution Overview
![Slide 4](slides/Slide4.png)
## 🎥 Live Demo
[![Watch the Demo](assets/etl_pipeline.png)](https://youtu.be/QnYO6jEtZjE)
[![Watch the Demo](assets/dash.png)](https://youtu.be/Mv3E31pOVcQ)
![Slide 7](slides/Slide7.png)
![Slide 8](slides/Slide8.png)
![Slide 9](slides/Slide9.png)

---

## 🚧 Known issues
### Incremental loading 
Our incremental loading needs to be ran relatively often once all the data is loaded otherwise the data won't be included in the refresh.
Another issue we have not tackled due to the limiting rates of the fred API is a check if any of the data has been change in the past: this will not be captured and could cause issues with some of our predictive models.

### Dash App
The Dash app struggled to incorporate some of the python files which included the models. To go past this obstacle, we made the decision to run some of the models individually and save the results in the data and assets folders. This means that unless they are not ran by the user, the Dash app will not have the updated data or model from the S3 instance from which the models are pulling the data.

### Models
The currency exchange rate predictive model does not work the same in the Dash and originial file, our goal was mainly to predict whether it would go up or down so the version in Dash is sufficient for that. 
The VAR model is not a consistent model: it is currently more of a test jupyer notebook as proof that a long loading of the data can still eventually make it to the dash app.

---

## 🧑‍💻 Authors
[Chirayu Betkekar](https://www.linkedin.com/in/chirayu-betkekar/)
[David (Sin Myung) Han](https://www.linkedin.com/in/sinmyunghan/)
[Beckten Harkleroad](https://www.linkedin.com/in/beckten-harkleroad/)
[Rohan Soni](https://www.linkedin.com/in/rohansoni98/)
[Florent Lee](https://www.linkedin.com/in/florent-lee/)

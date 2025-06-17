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

## 📦 Setup (Docker)

```bash
# Build and start the services
docker build -t dash .
docker run -p 8050:8050 dash
```

![Slide 1](slides/Slide1.png)
![Slide 2](slides/Slide2.png)
![Slide 3](slides/Slide3.png)
## Solution Overview
![Slide 4](slides/Slide4.png)
## Live Demo
[![Watch the Demo](assets/etl_pipeline.png)](https://youtu.be/QnYO6jEtZjE)
[![Watch the Demo](assets/dash.png)](https://youtu.be/Mv3E31pOVcQ)
![Slide 7](slides/Slide7.png)
![Slide 8](slides/Slide8.png)
![Slide 9](slides/Slide9.png)

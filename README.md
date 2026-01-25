
# 🏭 Smart Manufacturing Machines Efficiency Prediction

An end-to-end **Machine Learning + MLOps** project that predicts **manufacturing machine efficiency** using operational data and deploys the model using a **fully automated CI/CD pipeline** on **Google Cloud Platform (GCP)**.

---

## 🚀 Project Overview

This project demonstrates how smart manufacturing systems can leverage machine learning to monitor and predict machine efficiency.  
It covers the **complete ML lifecycle**, from data preprocessing and model training to production deployment using **Docker, Jenkins, Kubernetes, and ArgoCD**.

---

## 🧠 Tech Stack

### Machine Learning
- Python
- Pandas, NumPy
- Scikit-learn
- Xgboost
- Jupyter Notebook

### Backend & API
- Flask
- REST API

### Frontend
- HTML
- Tailwind CSS

### MLOps & DevOps
- Docker
- Jenkins (CI)
- GitHub Webhooks
- ArgoCD (CD)
- Kubernetes (Minikube)
- Google Cloud Platform (VM)

---

## 📂 Project Structure

```
Smart-Manufacturing-Machines-Efficiency-Prediction/
│
├── artifacts/                  # Trained models & preprocessors
├── notebooks/                  # Jupyter notebooks (EDA & experiments)
├── src/
│   ├── data_processing.py
│   ├── model_training.py
│   ├── prediction_pipeline.py
│   └── app.py                  # Flask app
│
├── Dockerfile
├── Jenkinsfile
├── deployment.yaml             # Kubernetes manifest
├── requirements.txt
├── setup.py
└── README.md
```

---

## 🔁 ML Workflow

1. **Data Processing**
   - Data cleaning & validation
   - Feature engineering
   - Scaling & encoding

2. **Model Training**
   - Model selection & training
   - Performance evaluation
   - Model persistence

3. **Prediction Pipeline**
   - Load trained model & preprocessors
   - Run real-time inference

4. **Flask Application**
   - REST API for predictions
   - JSON input/output support

---

## 📊 Model Performance

- Accuracy Score :0.99835
- Precision Score :0.9983523263996418
- Recall Score :0.99835
- F1 Score Score :0.998349164642211

---


## ⚙️ CI/CD Pipeline

### Continuous Integration (CI)
- Triggered by GitHub commits
- Jenkins pipeline:
  - Code checkout
  - Dependency installation
  - Testing
  - Docker image build & push

### Continuous Deployment (CD)
- ArgoCD monitors Git repository
- Automatic deployment to Kubernetes
- GitOps-based workflow

---

## ☁️ Cloud & Deployment

- Jenkins hosted on **Google Cloud VM**
- Kubernetes cluster using **Minikube / GKE**
- Automated deployments using **ArgoCD**

---

## 🧪 Run Locally

```bash
git clone https://github.com/<Sumit-Prasad01>/Smart-Manufacturing-Machines-Efficiency-Prediction.git
cd Smart-Manufacturing-Machines-Efficiency-Prediction
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python src/app.py
```

---

## 🔮 API Example

**Endpoint**
```
POST /predict
```

**Input**
```json

  "Operation_Mode", "Temperature_C", "Vibration_Hz",
  "Power_Consumption_kW", "Network_Latency_ms", "Packet_Loss_%",
  "Quality_Control_Defect_Rate_%", "Production_Speed_units_per_hr",
  "Predictive_Maintenance_Score", "Error_Rate_%","Year", "Month", "Day", "Hour"

```

**Output**
```json

  "Machine Efficiency": "Low/Medium/High"

```

---

## 📈 Key Highlights

- End-to-end ML & MLOps pipeline
- CI/CD with Jenkins & ArgoCD
- Dockerized & Kubernetes-ready
- Cloud-native scalable architecture

---




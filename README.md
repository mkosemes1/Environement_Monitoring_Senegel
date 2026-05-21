#  Sénégal Environmental Monitoring Pipeline

Pipeline Big Data temps réel : scraping météo → Kafka → MinIO (Parquet) → Dashboard.

## Architecture

```
Open-Meteo API  ─┐
                  ├──► producer.py ──► Kafka (3 brokers) ──► store_to_minio.py ──► MinIO (Parquet)
OpenWeatherMap  ─┘                                      └──► app.py (Dashboard Streamlit)
                                                                      ↑
                                                              JupyterLab (libre accès)
```

## Services

| Service          | Port  | Description                                    |
|------------------|-------|------------------------------------------------|
| Kafka broker 1   | 9092  | Broker principal                               |
| Kafka broker 2   | 9093  | Broker réplique                                |
| Kafka broker 3   | 9094  | Broker réplique                                |
| MinIO API        | 9000  | Stockage objet S3-compatible (Parquet)         |
| MinIO Console    | 9001  | Interface web MinIO                            |
| Dashboard        | 8501  | Streamlit — visualisation temps réel           |
| JupyterLab       | 8888  | Notebooks d'analyse (token: dakar2024)         |

## Démarrage rapide

```bash
# 1. Cloner et configurer
cp .env.example .env   # ou éditer directement .env

# 2. Lancer tous les services
docker compose up --build -d

# 3. Attendre ~30s que Kafka soit healthy, puis vérifier
docker compose ps

# 4. Accéder aux interfaces
#  Dashboard  → http://localhost:8501
#  MinIO      → http://localhost:9001  (minioadmin / minioadmin)
#  JupyterLab → http://localhost:8888  (token: dakar2024)
```

## Structure du projet

```
dakar-monitoring/
├── .env                        # Variables sensibles (ne pas committer)
├── .gitignore
├── docker-compose.yml
├── producer/
│   ├── producer.py             # Scraping Open-Meteo + OWM → Kafka
│   ├── store_to_minio.py       # Kafka → MinIO (format Parquet)
│   ├── Dockerfile.producer
│   └── Dockerfile.worker
├── consumer/
│   ├── app.py                  # Dashboard Streamlit temps réel
│   └── Dockerfile.dashboard
└── notebooks/
    └── senegal_analysis_starter.ipynb
```

## Buckets MinIO

- **raw-data** : données brutes telles que reçues de Kafka
- **processed-data** : données nettoyées et validées

Chemin des fichiers : `{region_code}/{source}/year={Y}/month={M}/day={D}/{HHmmss}.parquet`

## Topic Kafka

- **senegal-meteo** : 3 partitions, réplication factor 3

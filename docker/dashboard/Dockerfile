# ============================================================
# CryptoLake Dashboard -- Streamlit
# ============================================================
# Pre-installs dependencies at build time so the container
# starts instantly without pip install on every restart.
# ============================================================
FROM python:3.11-slim

RUN pip install --no-cache-dir \
    streamlit==1.39.0 \
    requests==2.31.0 \
    plotly==5.24.1 \
    pandas==2.2.3

WORKDIR /app

# Code is mounted as a volume in docker-compose
CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0"]

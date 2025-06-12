FROM python:3.12

WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y gcc

# Copy only requirements first
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of the app
COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]



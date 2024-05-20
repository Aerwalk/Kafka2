FROM python:3.8
WORKDIR /app
COPY requirements.txt .
RUN pip install urllib3
RUN pip install Werkzeug==2.0.2
RUN pip install psycopg2-binary
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "api_service.py"]
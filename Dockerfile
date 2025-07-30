FROM python:3.12

RUN apt-get update && \
    apt-get install -y wget gnupg2 curl build-essential python3-dev && \
    curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" \
        > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --upgrade pip setuptools wheel

COPY . .
RUN pip install -r requirements.txt

CMD ["python", "/app/scripts/clasificar_eventos.py"]
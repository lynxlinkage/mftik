FROM docker:27-cli

RUN apk add --no-cache python3

COPY server.py /updater/server.py

WORKDIR /opt/mftik/deploy
ENV PYTHONUNBUFFERED=1

CMD ["python3", "/updater/server.py"]

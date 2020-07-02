FROM python:3.6-slim
WORKDIR /app
COPY requirements.txt /app
ADD . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
CMD ["python", "slack_bot.py"]

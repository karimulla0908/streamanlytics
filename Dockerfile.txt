FROM python:latest

ADD datagenerator.py .



RUN pip install requests

RUN pip install bs4

RUN pip install azure.eventhub

RUN pip install datetime

RUN pip install lxml


CMD ["python3","./datagenerator.py"]
FROM python:3.8

WORKDIR /usr/src/app

RUN pip install --upgrade pip
COPY requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y libpq-dev

COPY . /usr/src/app/

CMD ["python", "test_11.py"]

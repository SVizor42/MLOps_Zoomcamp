FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

RUN pip install -U pip && pip install pipenv

COPY [ "Pipfile", "Pipfile.lock", "./" ]

RUN pipenv install --system --deploy

COPY [ "starter.py", "./" ]

CMD [ "python", "starter.py", "--taxi_type=fhv", "--year=2021", "--month=4"]
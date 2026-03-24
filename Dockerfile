FROM public.ecr.aws/lambda/python:3.12

WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY crawlers/   crawlers/
COPY model/      model/
COPY video/      video/
COPY report/     report/
COPY ingestion/  ingestion/
COPY scheduler/  scheduler/
COPY lambda_handler.py .

CMD ["lambda_handler.handler"]

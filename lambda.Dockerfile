FROM public.ecr.aws/lambda/python:3.8

RUN python3.8 -m pip install wheel nltk && \
    yum install -y make glibc-devel gcc patch mysql-devel

COPY requirements.txt /tmp/
RUN python3.8 -m pip install -r /tmp/requirements.txt -t .

COPY tacostats /var/task/tacostats
COPY lambda.env /var/task/.env

# Command can be overwritten by providing a different command in the template directly.
CMD ["tacostats.stats.lambda_handler"]

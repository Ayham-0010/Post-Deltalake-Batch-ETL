# ####################### BUILD ##########################

# FROM python:3.11-slim as build

# RUN apt-get update
# RUN apt-get install -y --no-install-recommends \
#     build-essential gcc 

# WORKDIR /usr/app

# RUN python -m venv /usr/app/venv
# ENV PATH="/usr/app/venv/bin:$PATH"

# COPY requirements.txt .
# RUN pip install -r requirements.txt

# ####################### PRODUCTION ##########################

# FROM python:3.11-slim

# WORKDIR /usr/app

# COPY --from=build /usr/app/venv ./venv
# COPY . .

# ENV PATH="/usr/app/venv/bin:$PATH"

# CMD [ "python", "src/index.py" ]

FROM spark:3.5.0

USER root
RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean

RUN pip3 install esdbclient

COPY /src /opt/spark/src


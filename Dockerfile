FROM python:3.14-bookworm

WORKDIR /opt/yaluk-http-server/

COPY requirements.txt /opt/yaluk-http-server/

RUN pip install --no-cache-dir -r requirements.txt

COPY src/main.py /opt/yaluk-http-server/main.py
COPY src/templates /opt/yaluk-http-server/templates

RUN groupadd -r yaluk && useradd -r -g yaluk -d /opt/yaluk-http-server yaluk \
    && mkdir -p /opt/yaluk \
    && chown -R yaluk:yaluk /opt/yaluk-http-server /opt/yaluk

USER yaluk

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

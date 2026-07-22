FROM python:3.13-alpine AS builder

WORKDIR /build

COPY tasks/webapi/requirements.txt ./

RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.13-alpine AS runtime

RUN addgroup -S appuser && adduser -S -G appuser -h /usr/src/app -s /sbin/nologin appuser

COPY --from=builder /opt/venv /opt/venv

WORKDIR /usr/src/app

COPY tasks/webapi/*.py ./
COPY shared/ ./shared/
COPY tasks/shared/ ./shared/
COPY infrastructure/ ./infrastructure/

RUN chown -R appuser:appuser /usr/src/app

USER appuser

ENV PATH="/opt/venv/bin:$PATH"

CMD [ "python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000" ]
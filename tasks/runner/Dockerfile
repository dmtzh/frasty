FROM python:3.13-alpine AS builder

WORKDIR /build

COPY tasks/runner/requirements.txt ./

RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt && \
    /opt/venv/bin/pip install --no-cache-dir "faststream[cli]"

FROM python:3.13-alpine AS runtime

RUN addgroup -S appuser && adduser -S -G appuser -h /usr/src/app -s /sbin/nologin appuser

COPY --from=builder /opt/venv /opt/venv

WORKDIR /usr/src/app

COPY shared/ ./shared/
COPY tasks/shared/ ./shared/
COPY infrastructure/ ./infrastructure/
COPY tasks/runner/ ./
RUN rm requirements.txt

RUN chown -R appuser:appuser /usr/src/app

USER appuser

ENV PATH="/opt/venv/bin:$PATH"

CMD [ "faststream", "run", "main:app" ]
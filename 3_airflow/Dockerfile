FROM quay.io/astronomer/astro-runtime:11.9.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.9.0 && deactivate
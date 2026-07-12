# Credit risk forecast

End-to-end **credit scoring** and **MLOps** reference: train a **LightGBM** pipeline, track experiments in **MLflow**, serve predictions from a **FastAPI** service, stream scoring traffic through **Kafka**, and persist decision outcomes to a **lakehouse** path using **PySpark** against a shared **Hive Metastore** and **S3-compatible** object storage (**MinIO**). Orchestration for scheduled monitoring and conditional retraining is handled by **Apache Airflow**; a full **CI/CD pipeline** under [`.github/`](.github/) runs tests on every PR, publishes the scoring API image on merge, and can trigger remote retraining via **GitHub Actions**.

This folder is designed to sit beside the rest of the workspace so you can wire the same networks, catalogs, and agent tooling into one coherent analytics and ML story.

---

## What this repository contains

| Area | Role |
|------|------|
| [`prod/lgbm_prod.py`](prod/lgbm_prod.py) | Production API: loads a registered model from **MLflow**, `/predict` scoring, optional **Kafka** consumer, **Spark** batch writes for prediction logging. |
| [`dags/`](dags/) | **Airflow** DAG definitions and callable pipelines (`pipelines/train_model.py`, drift / decay checks). |
| [`docker-compose.yml`](docker-compose.yml) | Core stack: Postgres + **MLflow** (with auth), **MinIO**, scoring API, **Kafka**; attaches to external Docker networks shared with other projects. |
| [`docker-compose.airflow.yml`](docker-compose.airflow.yml) | **Apache Airflow** 3.x (Celery executor): scheduler, workers, API server, and DAG volume mounts for training and **NannyML**-style monitoring. |
| [`client/kafka_request_generator.py`](client/kafka_request_generator.py) | Helper to exercise the async scoring path. |
| [`docker/nannyml-evaluator/`](docker/nannyml-evaluator/) | Containerized evaluation script used from Airflow’s **DockerOperator**. |
| [`.github/`](.github/) | **CI/CD pipeline** (GitHub Actions): PR tests, **GHCR** image publish, and **Airflow** retrain triggers — see [CI/CD pipeline](#cicd-pipeline) below. |

Training reads labeled tables (CSV/Parquet, including **S3** URIs backed by MinIO), fits a sklearn **Pipeline** with **LGBMClassifier**, logs metrics and registers the model in **MLflow**.

---

## Apache Airflow

**Airflow** is the control plane for **repeatable ML workflows**:

- **`credit_model_training_and_nannyml_monitoring`** — daily schedule: runs drift / model-decay monitoring (via **DockerOperator** + the NannyML evaluator image), branches on the result, and can **TriggerDagRun** the retrain DAG when retraining is warranted.
- **`credit_model_retrain_from_github_actions`** — on-demand / **GitHub Actions**–friendly: runs `pipelines/train_model.py` with environment injected from `pipelines/mlflow_training_env.py` to register a new model in **MLflow**.

Bring the Airflow stack up with `docker-compose.airflow.yml` (see that file for services: Postgres, Redis, scheduler, workers, API server, init). DAG code lives under [`dags/`](dags/). Workers receive MinIO credentials and data paths via environment variables so training and monitoring jobs read/write the same artifact layout as the main compose stack.

---

## CI/CD pipeline

The [`.github/workflows/ci-cd.yml`](.github/workflows/ci-cd.yml) workflow is the delivery path from code change to deployed scoring service and optional model retrain. It runs on **pull requests** and **pushes** to `main` / `master`, and supports manual **`workflow_dispatch`** for on-demand retrains without a merge.

```mermaid
flowchart LR
  PR["Pull request"] --> CI["ci: pytest"]
  PUSH["Push to main"] --> CI
  CI --> CD["cd: build & push image"]
  CI --> AF["trigger-airflow: retrain DAG"]
  WD["workflow_dispatch"] --> AF
  CD --> GHCR["ghcr.io/.../credit-api"]
  AF --> DAG["credit_model_retrain_from_github_actions"]
  DAG --> MLF["MLflow registry"]
```

| Job | When it runs | What it does |
|-----|--------------|--------------|
| **`ci`** | Every PR and push | Installs [`requirements-ci.txt`](requirements-ci.txt), runs `pytest` against [`tests/`](tests/) with `SKIP_MODEL_LOAD=true`. |
| **`cd`** | Push to `main` / `master` (after `ci` passes) | Builds [`prod/dockerfile`](prod/dockerfile) and pushes **`ghcr.io/<repo>/credit-api`** tags (`latest`, branch, SHA) to **GitHub Container Registry**. |
| **`trigger-airflow`** | Push to `main` / `master`, or `workflow_dispatch` (after `ci` passes) | On a **self-hosted runner** with VPN access, authenticates to **Airflow 3** (JWT), triggers **`credit_model_retrain_from_github_actions`**, and polls until the DAG run succeeds or fails. |

**Secrets and runner setup** live under [`.github/secrets/`](.github/secrets/): configure `AIRFLOW_API_URL`, `AIRFLOW_API_USERNAME`, and `AIRFLOW_API_PASSWORD` (see [`.github/secrets/README.md`](.github/secrets/README.md) and `scripts/sync-github-airflow-secrets.sh`). The `trigger-airflow` job requires a runner labeled `self-hosted`, `Linux`, `X64`, and `vpn` so it can reach a VPN-only Airflow API endpoint.

On merge, the typical path is: **tests pass → API image published → retrain DAG runs → new model registered in MLflow** — closing the loop between application code and model artifacts without manual deploy steps.

---

## How this aligns with the rest of the workspace

The following sibling directories are **not** submodules of this repo; they are **companion stacks** you run and connect with Docker networks and environment variables.

### [`spark-cluster`](../spark-cluster)

The Spark **master** and **worker** compose file joins external networks `minio_minio` and **`credit_risk_shared`**, matching [`docker-compose.yml`](docker-compose.yml) in this project. The scoring API defaults (`SPARK_MASTER_URL`, `SPARK_HIVE_METASTORE_URIS`, `SPARK_SQL_WAREHOUSE_DIR`) assume executors can reach **`spark-master`** and a **Hive Metastore** thrift endpoint, with warehouse data on **`s3a://`** backed by MinIO. Use this cluster when you want distributed writes and Spark UI visibility for prediction logging and ETL-sized jobs.

### [`trino`](../trino)

The **Trino** stack (query engine, **Hive Metastore**, **pgvector**, etc.) is another consumer of the same **MinIO**-backed lakehouse idea: federated SQL over Iceberg-style catalogs and companion stores. After this service writes **prediction events** and related tables through Spark, **Trino** is the natural place for **ad hoc SQL**, BI, and data quality checks on the same namespaces.

### [`llm`](../llm)

The **LLM Analytics Assistant** demonstrates **natural language → Trino SQL** (via LangChain and MCP), **RAG** over documents, and **LoRA** fine-tuning with **MLflow** tracking in notebooks. Conceptually:

- **This project** owns **batch training**, **registry promotion**, **online scoring**, and **Airflow**-driven **monitoring / retrain** loops.
- **`llm`** sits on the **analyst and agent** side: asking questions and generating read-only SQL against the warehouse **Trino** exposes—including tables populated or enriched by the credit-risk pipeline.

Shared themes: **MLflow** for experiment lineage, **MinIO**-compatible **S3** paths for artifacts and data, and **Trino** as the read path for structured truth.

### [`mcp`](../mcp)

The **`mcp`** folder hosts a **Model Context Protocol** server (**`trino_mcp.py`**) that exposes **Trino** as tools for IDEs and agents. That is the bridge between **conversational interfaces** (for example notebooks in **`llm`**) and **live warehouse metadata and SQL execution**. Point MCP clients at this server with the same **Trino** host and credentials you use for the **`trino`** compose stack so agents query the same catalogs this pipeline ultimately feeds.

---

## End-to-end picture

```mermaid
flowchart LR
  subgraph gha["GitHub Actions (.github)"]
    CI["ci: pytest"]
    CD["cd: GHCR publish"]
    GHA_AF["trigger-airflow"]
    CI --> CD
    CI --> GHA_AF
  end

  subgraph airflow["Apache Airflow"]
    DAG1["Monitoring DAG"]
    DAG2["Retrain DAG"]
    DAG1 --> DAG2
  end

  subgraph crf["credit_risk_forecast"]
    MLF["MLflow + MinIO"]
    API["FastAPI scoring"]
    KF["Kafka"]
    MLF --> API
    KF --> API
  end

  subgraph spark["spark-cluster"]
    SP["Spark workers"]
  end

  subgraph wh["trino + lakehouse"]
    TR["Trino"]
    HMS["Hive Metastore"]
    TR --> HMS
  end

  subgraph agents["llm + mcp"]
    MCP["Trino MCP"]
    LLM["LLM / LangChain"]
    LLM --> MCP
    MCP --> TR
  end

  GHA_AF --> DAG2
  CD --> API
  DAG1 --> MLF
  DAG2 --> MLF
  API --> SP
  SP --> HMS
  API --> MLF
```

---

## Quick start pointers

1. Ensure external Docker networks exist (names used in compose: **`minio_minio`**, **`credit_risk_shared`**) and that **MinIO** / **Hive Metastore** / **Spark** are reachable as configured in your `.env`.
2. From this directory: `docker compose up` for **MLflow**, **API**, **Kafka**, and dependencies (see [`docker-compose.yml`](docker-compose.yml)).
3. For **Airflow**: `docker compose -f docker-compose.airflow.yml up` after aligning environment variables with your MinIO and API endpoints (see comments and defaults in that file).
4. Run tests locally (same as the **`ci`** job): `pip install -r requirements-ci.txt && python -m pytest -q tests` (see [`pytest.ini`](pytest.ini)).
5. Configure [`.github/secrets/`](.github/secrets/) and a VPN-capable self-hosted runner if you want **`trigger-airflow`** to reach your Airflow API from GitHub Actions.

For deeper notebook and MCP setup, follow [`../llm/README.md`](../llm/README.md). For the query engine and catalog layout, see [`../trino/README.md`](../trino/README.md).

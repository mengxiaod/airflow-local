# Airflow Local — Project Explained

## Project1: hello_world

- dockerfile, docck-compose, containers, DAG 

### Why No Dockerfile?

Because the compose file uses the **official pre-built image** from Docker Hub:

```yaml
image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.2.2}
```

Apache publishes a ready-made image with Airflow already installed. You only need a `Dockerfile` when you want to customize that image (e.g., install extra Python packages permanently). The comment on line 50-53 of `docker-compose.yaml` tells you exactly how to switch to that mode — comment out `image:`, uncomment `build: .`, then add a Dockerfile.

---

### The 8 Containers Defined

The compose file spins up **8 services** (containers), all running the same `apache/airflow:3.2.2` image but with different roles:

```
┌─────────────────────────────────────────────────────────────────┐
│                        Your Machine                             │
│                                                                 │
│  ┌──────────┐   ┌──────────┐                                   │
│  │ postgres │   │  redis   │  ← Infrastructure layer           │
│  │ (DB)     │   │ (broker) │                                   │
│  └────┬─────┘   └────┬─────┘                                   │
│       │              │                                          │
│  ┌────▼──────────────▼──────────────────────────────────┐      │
│  │              airflow-init (runs once, exits)          │      │
│  │     Creates DB tables, admin user, checks resources   │      │
│  └───────────────────────────────────────────────────────┘      │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐                      │
│  │ airflow-         │  │ airflow-         │                     │
│  │ apiserver        │  │ dag-processor    │                     │
│  │ :8080 (Web UI)   │  │ (reads DAG files)│                     │
│  └─────────────────┘  └─────────────────┘                      │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐                      │
│  │ airflow-         │  │ airflow-         │                     │
│  │ scheduler        │  │ worker           │                     │
│  │ (decides when)   │  │ (actually runs   │                     │
│  └─────────────────┘  │  the tasks)       │                     │
│                        └─────────────────┘                      │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐                      │
│  │ airflow-         │  │ airflow-cli /   │                      │
│  │ triggerer        │  │ flower          │                      │
│  │ (async sensors)  │  │ (optional tools)│                      │
│  └─────────────────┘  └─────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

| Container | Role |
|---|---|
| `postgres` | Stores all Airflow metadata (DAG runs, task states, logs) |
| `redis` | Message broker — scheduler publishes tasks here, workers pick them up |
| `airflow-init` | One-shot bootstrap: runs DB migrations, creates admin user, then exits |
| `airflow-apiserver` | Serves the Web UI at `localhost:8080` |
| `airflow-dag-processor` | Reads and parses your `.py` files in `/dags/` |
| `airflow-scheduler` | Decides when to trigger DAG runs based on schedules |
| `airflow-worker` | Actually executes the tasks (Celery worker) |
| `airflow-triggerer` | Handles deferred/async operators |

---

### How the Shared Config Block (`x-airflow-common`) Works

Lines 47-90 of `docker-compose.yaml` define a **YAML anchor** (`&airflow-common`) that is reused by every Airflow container via `<<: *airflow-common`. This avoids repeating the same environment variables, volumes, and dependencies 6 times.

Key shared settings:
- **Environment vars** — tells every container how to connect to postgres and redis
- **Volumes** — the critical part that links your files into the containers

```yaml
volumes:
  - ./dags:/opt/airflow/dags      # your DAG files → inside container
  - ./logs:/opt/airflow/logs      # logs come back out to your machine
  - ./config:/opt/airflow/config
  - ./plugins:/opt/airflow/plugins
```

---

### How DAG Files Get Into Containers (Bind Mount)

`hello_world.py` is **never mentioned by name** in `docker-compose.yaml` and is **never copied** into the containers. The relevant line is:

```yaml
- ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
```

This is a **bind mount** — it exposes your local `./dags/` folder directly inside the container at `/opt/airflow/dags/`. Both sides point to the same bytes on disk.

| | Copy | Bind Mount (what this project uses) |
|---|---|---|
| How | Files duplicated into the image at build time | Folder on your machine directly exposed to the container |
| If you edit the file | Container sees the old version | Container sees the change **instantly** |
| Mentioned in compose | Specific filenames | Just the folder path |
| Requires rebuild? | Yes | No |

Because `x-airflow-common` is applied to all Airflow services via `<<: *airflow-common`, every container gets that same volume mount. So `./dags/` on your machine becomes `/opt/airflow/dags/` in all 6+ Airflow containers simultaneously — they all read from the same location on your hard drive.

`hello_world.py` just happens to live inside `./dags/`. You could add `./dags/another_dag.py` right now and every container would see it immediately, no restart needed.

---

### How `hello_world.py` Gets Triggered to Run

### Step 1: Always Happens First — DAG Registration

The **dag-processor** container constantly scans `/opt/airflow/dags/`, imports `hello_world.py` as a Python module, and registers the DAG definition into **postgres**. This happens automatically whenever you save the file.

After this, Airflow "knows about" `hello_world` — but it hasn't run anything yet.

---

#### Path A: Scheduler Triggers It Automatically

```
hello_world.py has schedule="@daily"
        │
        ▼
airflow-scheduler checks postgres every few seconds:
"Is it time to run hello_world?" → YES (daily schedule elapsed)
        │
        ▼
scheduler creates a DAG Run + TaskInstance records in postgres
scheduler pushes task messages onto redis queue
        │
        ▼
airflow-worker is always listening to redis
worker picks up "print_date" task → executes the BashOperator
worker picks up "greet" task → executes the PythonOperator
        │
        ▼
worker writes results back to postgres + ./logs/
```

---

#### Path B: You Manually Trigger It from the Web UI

```
You click "Trigger DAG" at localhost:8080
        │
        ▼
airflow-apiserver receives the request
apiserver creates a DAG Run record in postgres (marked as manual)
        │
        ▼
airflow-scheduler sees the new DAG Run in postgres
scheduler pushes task messages onto redis queue
        │
        ▼
airflow-worker picks up and executes tasks (same as above)
```

---

### The Key Insight: Roles Are Separated

No single container does everything. Each has one job:

| Container | Job |
|---|---|
| `dag-processor` | Parse Python files → register DAG structure in postgres |
| `scheduler` | Decide WHEN to run → push task messages to redis |
| `worker` | Actually execute the task code (your `greet()` function, your bash command) |
| `apiserver` | Accept manual triggers from UI, show results |
| `redis` | The queue that decouples scheduler from worker |
| `postgres` | Shared memory for all containers — stores DAG state, task state, logs metadata |

The worker is the only container that actually **runs your Python code**. The scheduler never executes tasks — it only decides timing and queues work. This separation is what allows you to scale workers independently.

---

### How `hello_world.py` Gets Executed (Full Chain)

This is the key chain:

```
hello_world.py (your machine: ./dags/)
        │
        │  volume mount (bind mount)
        ▼
/opt/airflow/dags/hello_world.py (inside every container)
        │
        │  parsed by
        ▼
airflow-dag-processor → registers DAG "hello_world" into postgres
        │
        │  scheduler reads postgres, sees schedule="@daily"
        ▼
airflow-scheduler → creates a DAG run, pushes task messages to redis
        │
        │  worker listens to redis
        ▼
airflow-worker → pulls task, executes BashOperator ("print_date")
              → then executes PythonOperator ("greet")
        │
        │  results written back to postgres + logs/
        ▼
airflow-apiserver → you see "success" in the Web UI at localhost:8080
```

**The bind mount is the magic link.** You write Python on your machine in `./dags/` — Docker makes that same folder appear at `/opt/airflow/dags/` inside every container simultaneously. No rebuild, no copy. Save the file and the dag-processor picks it up within seconds.

---

### Startup Order (Dependency Chain)

The `depends_on` blocks enforce this order:

```
postgres + redis (healthy)
        ↓
airflow-init (completed successfully)
        ↓
apiserver + scheduler + dag-processor + worker + triggerer (all start)
```

None of the Airflow services start until `airflow-init` finishes creating the database schema and admin user.

## Project2: spotify_etl_orchestration

A daily Airflow DAG (`spotify_lambda_trigger`) that orchestrates a three-stage Spotify ETL pipeline on AWS.

**Pipeline stages:**

1. **Extract** — Invokes the `spotify_api_data_extract` Lambda function (us-east-1) to pull raw data from the Spotify API and land it in S3.
2. **Sense** — Polls the S3 raw-data prefix (`raw_data/to_processed/`) every 60 seconds (up to 1 hour) using `S3KeySensor`, blocking until the file appears.
3. **Transform** — Triggers the `spotify_transformation_job` Glue job, which runs a PySpark transformation script stored in S3 under the `aws-glue-assets` bucket, using the `spotify_glue_iam_role`.

```
trigger_extract_lambda >> check_s3 >> trigger_glue
```

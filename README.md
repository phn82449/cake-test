# SFTP to SFTP Synchronization Airflow Project

This project implements an Apache Airflow DAG to unidirectionally synchronize files from a source SFTP server to a target SFTP server, preserving the directory structure.
After a successful transfer, the source file is automatically deleted. The project leverages **Airflow 3.1.7** running in a Docker Compose stack.

## Requirements

- Docker and Docker Compose
- [uv](https://github.com/astral-sh/uv) (Python package manager)

## Quick Start Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/phn82449/cake-test.git
   cd cake-test
   ```

2. **Start the Infrastructure:**
   The project uses Docker Compose to run Airflow (API Server, Scheduler, Worker, Triggerer, DAG Processor) with a Celery executor, backed by PostgreSQL and Redis. It also spins up two mock SFTP servers for testing.

   ```bash
   docker compose up -d
   ```

3. **Initialize the Environment:**
   Run the following commands to create an admin user and set up the necessary SFTP connections automatically:

   **Create an Admin User:**
   ```bash
   docker compose exec airflow-scheduler airflow users create \
     --username admin \
     --password admin \
     --firstname admin \
     --lastname admin \
     --role Admin \
     --email admin@superhero.org
   ```

   **Add Source and Target Connections:**
   ```bash
   docker compose exec airflow-scheduler airflow connections add 'sftp_source_conn' \
     --conn-type 'sftp' \
     --conn-host 'sftp-source' \
     --conn-login 'sourceuser' \
     --conn-password 'sourcepass' \
     --conn-port '22'
     
   docker compose exec airflow-scheduler airflow connections add 'sftp_target_conn' \
     --conn-type 'sftp' \
     --conn-host 'sftp-target' \
     --conn-login 'targetuser' \
     --conn-password 'targetpass' \
     --conn-port '22'
   ```

4. **Access Airflow UI:**
   - URL: `http://localhost:8080`
   - You can now log in using the credentials you just created:
     - Username: `admin`
     - Password: `admin`

5. **Test the Sync:**
   - Drop files into `./data/source` on your local machine.
   - Trigger the `sftp_to_sftp_sync` DAG in the Airflow UI.
   - Check `./data/target` to see the synced files.
   - The files in `./data/source` will be deleted automatically upon successful transfer.

## Architectural Decisions & Trade-offs

### 1. Extensibility via Abstraction

Instead of writing a rigid, single-purpose SFTP-to-SFTP script, the project introduces abstract base classes (`BaseStorageReader`, `BaseStorageWriter`, `BaseTransformer`).
- **Adaptability:** If business requirements change to pull from Amazon S3 instead of SFTP, you only need to implement an `S3Reader` that inherits from `BaseStorageReader`. The `ExtensibleTransferOperator` remains completely unchanged.
- **Extensibility:** The operator accepts a list of `BaseTransformer` objects. If data needs to be compressed, encrypted, or scrubbed for PII before landing in the target, you can inject these transformers seamlessly into the data stream.

### 2. Handling Large Files (Scalability)

To accommodate files scaling from kilobytes to gigabytes:
- **Streaming Chunks:** The implementation avoids loading files entirely into memory. Instead, it utilizes `shutil.copyfileobj` to stream data in chunks directly over the network sockets. This guarantees a flat memory footprint regardless of file size.

### 3. Source Deletion and State Management

- **Auto-Deletion:** To prevent reprocessing and save space, the `ExtensibleTransferOperator` explicitly deletes the source file (`reader.delete_file()`) immediately after a successful transfer.
- **Idempotency:** Because files are moved and then deleted from the source, the operation is naturally idempotent. If a file transfer fails halfway, it remains on the source and will be retried on the next DAG run.

### 4. Airflow 3 & Celery Executor

- The project has been updated to use **Airflow 3.1.7**, transitioning from the deprecated `webserver` command to `api-server` and `dag-processor`.
- For a scalable, production-like setup, this project utilizes PostgreSQL and the Celery Executor instead of SQLite, allowing for concurrent task execution across distributed workers.

## Linting and Code Quality

The codebase strictly adheres to the Ruff linter configuration provided, enforcing PEP8, type annotations, and Google-style docstrings.

To run the linter locally:
```bash
uv tool run ruff format .
uv tool run ruff check .
```

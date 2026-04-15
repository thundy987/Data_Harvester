# Data Harvester

A Python ETL pipeline that scans a Windows file system, extracts file and folder metadata, and loads it into a SQL Server migration database. This is the data collection layer of a broader SolidWorks PDM migration workflow.

```
Windows File System → [data-harvester] → SQL Server Migration DB → XML Export Tool → PDM Vault
```

---

## Why I Built This

As a SolidWorks PDM Data Management Specialist, I am responsible for migrating customer's existing data into their PDM vault. Getting customer files into a PDM vault requires a structured SQL Server database describing where every file lives and what it looks like. Doing that by hand is not realistic at scale.

I used this project to learn Python seriously for the first time. I wanted to understand how to structure a multi-module project, write a real test suite, and connect Python to SQL Server.

---

## Technical Challenges and Lessons

<details>
<summary><strong>1. Project Structure</strong></summary>

Coming from SQL, I was used to everything living in one place. Splitting logic across `sources/`, `pipeline/`, `db/`, and `utils/` packages took more effort than I expected, especially getting Python imports to work correctly across them. I learned about `__init__.py`, relative vs absolute imports, and why flat scripts don't scale.

</details>

<details>
<summary><strong>2. Separating Concerns</strong></summary>

My first instinct was to extract metadata and clean it in the same function. I pulled those apart so that scanning, transformation, and database writes are each handled independently. This made the code easier to change without breaking something else.

</details>

<details>
<summary><strong>3. Error Handling Strategy</strong></summary>

I had to decide what should stop the pipeline completely vs what should just get logged and skipped. A bad root path or a failed DB connection stops everything because there's no point continuing. A single locked file or unreadable metadata gets logged and skipped so the rest of the scan still completes.

</details>

<details>
<summary><strong>4. Learning New Libraries</strong></summary>

This was my first time using `pyodbc`, `pathlib`, `hashlib`, and `python-dotenv` in a real project. Each one required reading documentation and understanding not just how to call the functions but when to use them and why. `pathlib` in particular changed how I thought about working with file paths. Using an object with properties instead of manipulating strings was a big shift.

</details>

<details>
<summary><strong>5. Choosing the Right Data Structure</strong></summary>

Early on I was passing data around as tuples because that felt simple. As the pipeline grew I had to think harder about when a tuple was the right call vs a dictionary vs a custom object. The folder lookup table is a good example. Using a dictionary to map folder paths to IDs made the logic significantly cleaner than what I had started with.

</details>

<details>
<summary><strong>6. Knowing What to Leave Out</strong></summary>

I had to be honest about what I could actually build well in v1 vs what I should park for later. Batch inserts and Windows Authentication are the right next steps, but shipping a working, well-structured v1 first was the right call over trying to build everything at once and finishing nothing.

</details>

<details>
<summary><strong>7. MD5 Checksums</strong></summary>

I needed a way to detect duplicate files across a source system. MD5 is not cryptographically secure but it is fast and good enough for file deduplication in this context. I used Python's `hashlib.file_digest()` which reads the file in chunks and avoids loading large files fully into memory.

</details>

---

## Design Decisions

### Transformation in Python, not SQL

All data cleansing happens in the Python pipeline before anything is written to the database. The Migration DB is a load target, not a transformation workspace. This keeps business logic version-controlled and testable.

Post-load SQL transformations like deduplication or folder restructuring are still available to migration teams as a separate manual step, but they are outside the scope of this tool.

### Error Classification

Errors are split into recoverable and unrecoverable. A locked file or unreadable metadata gets logged and skipped so the pipeline keeps going. A failed DB connection or invalid root path halts the pipeline immediately because there is nothing useful to do after that.

### Idempotency

The pipeline checks that both target tables are empty before running. If records already exist, it refuses to proceed. This prevents accidental duplication. The user must pass `--clear` to truncate the tables before re-running.

### ID Generation

Folder and file IDs are generated using Python's `itertools.count()` rather than SQL identity columns. This keeps ID assignment in the application layer where it can be tested without a database. The counter resets if the module is reloaded, but that is not a concern for single-instance CLI use in v1.

---

## How It Works

1. Walks the file system from a user-supplied root directory
2. Builds a folder hierarchy and assigns each folder a unique ID
3. Extracts metadata for every file (name, path, size, dates, MD5 checksum)
4. Cleans the data in Python before writing to the database
5. Loads folders and files into the migration database
6. Logs pipeline activity via a stored procedure

The pipeline will not run if either target table already has data. The user must explicitly pass `--clear` to truncate both tables before re-running.

---

## Project Structure

```
data-harvester/
├── main.py                  # Entry point and argument parsing
├── db/
│   ├── connection.py        # SQL Server connection
│   └── repository.py        # All database read/write functions
├── sources/
│   ├── base.py              # Placeholder for future abstract base class
│   └── windows_fs/
│       ├── scanner.py       # Walks the file system, returns files and folders
│       └── metadata.py      # Extracts metadata from a single file
├── pipeline/
│   ├── orchestrator.py      # Coordinates the full pipeline run
│   └── transformer.py       # Data cleansing functions
├── utils/
│   ├── id_generator.py      # Generates folder and file IDs
│   └── logger.py            # Writes logs to file and console
├── tests/
│   ├── conftest.py          # Shared fixtures
│   ├── dummy_data/          # Static files with known MD5s for testing
│   ├── test_id_generator.py
│   ├── test_transformer.py
│   ├── test_scanner.py
│   ├── test_metadata.py
│   ├── test_repository.py
│   ├── test_orchestrator.py
│   └── test_integration.py
├── requirements.txt
└── README.md
```

---

## Requirements

- Python 3.14+
- SQL Server 2019+
- SolidWorks PDM Migration Database

### Python Dependencies

```
pyodbc
python-dotenv
pytest
pytest-mock
```

```bash
pip install -r requirements.txt
```

---

## Usage

```bash
python main.py --scan_root_directory "C:\path\to\files" --server_name SQLSERVER\INSTANCE --database_name Migration --sql_username sa --password yourpassword
```

To clear tables before re-running:

```bash
python main.py --scan_root_directory "C:\path\to\files" --server_name SQLSERVER\INSTANCE --database_name Migration --sql_username sa --password yourpassword --clear
```

Alternatively, store credentials in a `.env` file and run without arguments:

```bash
python main.py
```

---

## Running Tests

```bash
python -m pytest
```

Tests require a live SQL Server connection. Set credentials in `.env` before running. Use a test database because the suite wipes both tables on every run.

---

## Limitations

> V1 only supports Windows file systems, SQL Server authentication, and command-line execution. Inserts are row-by-row. There is no resume capability if the pipeline fails mid-run. SolidWorks custom file properties are not extracted because they require the Document Manager SDK. Concurrent pipeline instances are not supported. `Directories.Path` is not populated by the harvester. Run `RebuildDirectoryPaths` post-load if needed.

---

## Roadmap

- [ ] v2 — Batch inserts with configurable batch size
- [ ] v2 — Windows Authentication support
- [ ] v2 — Abstract base class for source systems (`sources/base.py`)
- [ ] v2 — SolidWorks Document Manager integration for custom file properties
- [ ] v3 — Incremental/delta load support
- [ ] v3 — Resume capability after mid-run failure
- [ ] v3 — Additional source systems (SolidWorks PDM, Windchill)
- [ ] v3 — GUI

---

## AI Usage Disclaimer

- Claude was used as a mentor and search utility, not a code generator.
- All code was written by me except the tests.
- It was used to explain concepts and ask questions that pushed me to think through problems before writing any code.

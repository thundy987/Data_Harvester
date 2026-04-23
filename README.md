# Data Harvester

A Python ETL pipeline that extracts file and folder metadata from source systems and loads it into a SQL Server migration database. This is the data collection layer of a broader SolidWorks PDM migration workflow.

```
Source System → [data-harvester] → SQL Server Migration DB → XML Export Tool → PDM Vault
```

---

## Why I Built This

As a SolidWorks PDM Data Management Specialist, I am responsible for migrating customer's existing data into their PDM vault. Getting customer files into a PDM vault requires a structured SQL Server database describing where every file lives and what it looks like. Doing that by hand is not realistic at scale.

I used this project to learn Python seriously for the first time. I wanted to understand how to structure a multi-module project, write a real test suite, and connect Python to SQL Server.

---

## Technical Challenges and Lessons

<details>
<summary><strong>1. Separating Concerns</strong></summary>

My first instinct was to extract metadata and clean it in the same function. I pulled those apart so that each source class owns its own extraction and transformation, and the pipeline only handles loading. This made it possible to add new source types without changing the pipeline.

</details>

<details>
<summary><strong>2. Error Handling Strategy</strong></summary>

I had to decide what should stop the pipeline completely vs what should just get logged and skipped. A bad root path or a failed DB connection stops everything because there's no point continuing. A single locked file or unreadable metadata gets logged and skipped so the rest of the scan still completes.

</details>

<details>
<summary><strong>3. Learning New Libraries</strong></summary>

This was my first time using `pyodbc`, `pathlib`, `hashlib` etc in a real project. Each one required reading documentation and understanding not just how to call the functions but when to use them and why. `pathlib` in particular changed how I thought about working with file paths. Using an object with properties instead of manipulating strings was a big shift.

</details>

<details>
<summary><strong>4. Choosing the Right Data Structure</strong></summary>

Early on I was passing data around as tuples because that felt simple. As the pipeline grew I had to think harder about when a tuple was the right call vs a dictionary vs a custom object. The folder lookup table is a good example. Using a dictionary to map folder paths to IDs made the logic significantly cleaner than what I had started with.

</details>

<details>
<summary><strong>5. Abstract Base Classes</strong></summary>

When I refactored the project to support multiple source types, I had to figure out what belonged in a shared contract vs what was specific to Windows file systems. Methods like `fetch_data()` that return a standard shape belong in the contract. Methods like `_extract_properties()` that use `rglob()` and `hashlib` belong in the implementation. Getting that boundary wrong would mean the next source type either can't conform to the contract or has to fight it.

The Python mechanics were a separate challenge. I had never worked with `ABC`, `@abstractmethod`, or `@property` before. Understanding how a parent class enforces structure on its children without dictating behavior took time and a lot of reading the docs and examples.

</details>

---

## Design Decisions

### Source Class Architecture

Each source system implements the `SourceSystem` abstract base class and is responsible for scanning, extracting metadata, and transforming data into the format the pipeline expects. The pipeline does not know or care how a source collects its data. It only expects a `fetch_data()` call that returns directory records and file records in the agreed-upon shape. Adding a new source type requires no changes to the pipeline or the database layer.

Post-load SQL transformations like deduplication or folder restructuring are still available to migration teams as a separate manual step, but they are outside the scope of this tool.

`transformer.py` contains shared cleansing functions currently only used by `WindowsFS`. If no other source ends up needing them, they may be moved into the `WindowsFS` class as private methods.

### Error Classification

Errors are split into recoverable and unrecoverable. A locked file or unreadable metadata gets logged and skipped so the pipeline keeps going. A failed DB connection or invalid root path halts the pipeline immediately because there is nothing useful to do after that.

### Idempotency

The pipeline checks that both target tables are empty before running. If records already exist, it refuses to proceed. This prevents accidental duplication.

### ID Generation

Folder and file IDs are generated using Python's `itertools.count()` rather than SQL identity columns. This keeps ID assignment in the application layer where it can be tested without a database. The counter resets if the module is reloaded, but that is not a concern for single-instance CLI use in v1.

---

## How It Works

1. The user runs `main.py` and is guided through an interactive CLI prompt
2. The CLI collects source type, credentials, target database connection, and batch size
3. The selected source class scans the data source, extracts metadata, and transforms it into the format the pipeline expects
4. The pipeline builds a folder hierarchy and assigns each folder a unique ID
5. Folders and files are loaded into the migration database in batches
6. Pipeline activity is logged via a stored procedure

The CLI validates connections and checks that target tables are empty before the pipeline runs. If tables contain data, the user is prompted to clear them.

---

## Project Structure

```
data-harvester/
├── main.py                  # Entry point
├── ui/
│   └── cli_prompt.py        # Interactive CLI setup (source, credentials, batch size)
├── db/
│   ├── __init__.py
│   ├── connection.py        # SQL Server connection
│   └── repository.py        # All database read/write functions
├── sources/
│   ├── __init__.py
│   ├── base.py              # Abstract base class for source systems
│   ├── windows_fs/
│   │   └── WindowsFS.py     # Windows file system source implementation
│   └── PDMDatabase/
│       ├── PDMDatabase.py   # SolidWorks PDM vault database source implementation
│       └── sql/             # SQL queries loaded at runtime
│           ├── get_folders.sql
│           └── get_files.sql
├── pipeline/
│   ├── __init__.py
│   ├── pipeline.py          # Coordinates the full pipeline run
│   └── transformer.py       # Data cleansing functions
├── utils/
│   ├── __init__.py
│   ├── id_generator.py      # Generates folder and file IDs
│   └── logger.py            # Writes logs to file and console
├── logs/                    # Timestamped log files created at runtime
├── tests/
│   ├── connection_test.py
│   ├── id_generator_test.py
│   ├── pdm_database_test.py
│   ├── pipeline_test.py
│   ├── repository_test.py
│   ├── transformer_test.py
│   └── windows_fs_test.py
├── pytest.ini
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
rich
pytest
pytest-mock
```

```bash
pip install -r requirements.txt
```

---

## Usage

```bash
python main.py
```

The tool walks you through source selection, credential entry, and batch size configuration via interactive prompts. No command-line arguments are required.

Each run creates a timestamped log file in the `logs/` directory. The log captures connection events, batch flushes, skipped records, and any errors encountered during the pipeline run.

---

## Testing

52 unit tests across 7 test files. No database connection required.

Tests use `unittest.mock` to replace real dependencies like db connections with `MagicMock` so tests run without a real db or external service. Filesystem tests use pytest's `tmp_path` fixture to create real temporary folders and files on disk

```bash
pytest
```

To run a specific test file:

```bash
pytest tests/repository_test.py
```

---

## Limitations

> SQL Server authentication only. There is no resume capability if the pipeline fails mid-run. SolidWorks custom file properties are not extracted because they require the Document Manager SDK. Concurrent pipeline instances are not supported. `Directories.Path` is not populated by the harvester. The Migration db has a stored procedure to populate that column. MD5 value cannot be harvested from a PDMDatabase alone, without its accompanying Archive folder.

---

## Roadmap

- [x] - Batch inserts with `--batch_size` argument (default: 1000)
- [x] - Abstract base class for source systems (`sources/base.py` and `sources/windows_fs/WindowsFS.py`)
- [x] - PDM database source handling
- [x] - Convert CLI arguments into rich prompt interaction.
- [x] - Write tests
- [ ] - API source handling
- [ ] - Incremental/delta load support
- [ ] - Resume capability after mid-run failure
- [ ] - GUI

---

## AI Usage Disclaimer

- Claude was used as a mentor and search utility, not a code generator.
- All code was written by me and only after I could clearly explain it.
- It was used to explain concepts and ask questions that pushed me to think through problems before writing any code.

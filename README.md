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
<summary><strong>1. Project Structure</strong></summary>

Coming from SQL, I was used to everything living in one place. Splitting logic across `sources/`, `pipeline/`, `db/`, and `utils/` packages took more effort than I expected, especially getting Python imports to work correctly across them. I learned about `__init__.py`, relative vs absolute imports, and why flat scripts don't scale.

</details>

<details>
<summary><strong>2. Separating Concerns</strong></summary>

My first instinct was to extract metadata and clean it in the same function. I pulled those apart so that each source class owns its own extraction and transformation, and the pipeline only handles loading. This made it possible to add new source types without changing the pipeline.

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
<summary><strong>6. Abstract Base Classes</strong></summary>

When I refactored the project to support multiple source types, I had to figure out what belonged in a shared contract vs what was specific to Windows file systems. Methods like `fetch_data()` that return a standard shape belong in the contract. Methods like `_extract_properties()` that use `rglob()` and `hashlib` belong in the implementation. Getting that boundary wrong would mean the next source type either can't conform to the contract or has to fight it.

The Python mechanics were a separate challenge. I had never worked with `ABC`, `@abstractmethod`, or `@property` before. Understanding how a parent class enforces structure on its children without dictating behavior took time and a lot of reading the docs and examples.

</details>

---

## Design Decisions

### Transformation in the Source, not SQL

All data cleansing happens in the source class before anything is passed to the pipeline. Each source is responsible for extracting its own data and transforming it into the format the pipeline expects. This keeps business logic version-controlled, testable, and source-agnostic.

Post-load SQL transformations like deduplication or folder restructuring are still available to migration teams as a separate manual step, but they are outside the scope of this tool.

### Source Class Architecture

Each source system implements the `SourceSystem` abstract base class and is responsible for scanning, extracting metadata, and transforming data into a standard format. The pipeline does not know or care how a source collects its data. It only expects a `fetch_data()` call that returns directory records and file records in the agreed-upon shape. Adding a new source type requires no changes to the pipeline or the database layer.

### Error Classification

Errors are split into recoverable and unrecoverable. A locked file or unreadable metadata gets logged and skipped so the pipeline keeps going. A failed DB connection or invalid root path halts the pipeline immediately because there is nothing useful to do after that.

### Idempotency

The pipeline checks that both target tables are empty before running. If records already exist, it refuses to proceed. This prevents accidental duplication. The user must pass `--clear` to truncate the tables before re-running.

### ID Generation

Folder and file IDs are generated using Python's `itertools.count()` rather than SQL identity columns. This keeps ID assignment in the application layer where it can be tested without a database. The counter resets if the module is reloaded, but that is not a concern for single-instance CLI use in v1.

---

## How It Works

1. A source class scans the data source and extracts metadata
2. The source class cleans and transforms the data into the format the pipeline expects
3. The pipeline builds a folder hierarchy and assigns each folder a unique ID
4. Folders and files are loaded into the migration database in batches
5. Pipeline activity is logged via a stored procedure

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
│   ├── base.py              # Abstract base class for source systems
│   ├── windows_fs/
│   │   └── WindowsFS.py     # Windows file system source implementation
│   └── pdm_database/
│       └── PDMDatabase.py   # SolidWorks PDM vault database source implementation
├── pipeline/
│   ├── orchestrator.py      # Coordinates the full pipeline run
│   └── transformer.py       # Data cleansing functions
├── utils/
│   ├── id_generator.py      # Generates folder and file IDs
│   └── logger.py            # Writes logs to file and console
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
```

```bash
pip install -r requirements.txt
```

---

## Usage

```bash
python main.py --source_location "C:\path\to\files" --server_name SQLSERVER\INSTANCE --database_name Migration --sql_username sa --password yourpassword
```

To clear tables before re-running:

```bash
python main.py --source_location "C:\path\to\files" --server_name SQLSERVER\INSTANCE --database_name Migration --sql_username sa --password yourpassword --clear
```

Alternatively, store credentials in a `.env` file and run without arguments:

```bash
python main.py
```

---

## Limitations

> SQL Server authentication only. There is no resume capability if the pipeline fails mid-run. SolidWorks custom file properties are not extracted because they require the Document Manager SDK. Concurrent pipeline instances are not supported. `Directories.Path` is not populated by the harvester. The Migration db has a stored procedure to populate that column. MD5 value cannot be harvested from a PDMDatabase alone, without it's accompanying Archive folder.

---

## Roadmap

- [x] v2 - Batch inserts with `--batch_size` argument (default: 1000)
- [x] v2 - Abstract base class for source systems (`sources/base.py` and `sources/windows_fs/WindowsFS.py`)
- [x] v2 - PDM database source handling
- [ ] v2 - API source handling
- [ ] v3 - SolidWorks Document Manager integration for custom file properties
- [ ] v3 - Incremental/delta load support
- [ ] v3 - Resume capability after mid-run failure
- [ ] v3 - Additional source systems (SolidWorks PDM, Windchill)
- [ ] v3 - GUI

---

## AI Usage Disclaimer

- Claude was used as a mentor and search utility, not a code generator.
- All code was written by me and only after I could clearly explain it.
- It was used to explain concepts and ask questions that pushed me to think through problems before writing any code.

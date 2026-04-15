# PDM Harvester

A Python ETL pipeline that harvests file and folder metadata from source systems and loads it into a SolidWorks PDM Migration Database, ready for import into a PDM vault.

---

## Background

Migrating files into SolidWorks PDM requires a structured metadata database that describes both the current physical location of files and their desired future state inside the vault. This tool automates the extraction and loading of that metadata from source file systems.

This project is the harvesting layer of a wider migration workflow:

```
Source System → [pdm-harvester] → Migration DB → XML Export Tool → PDM Vault
```

---

## Features

- Walks a Windows file system from a user-supplied root directory
- Extracts file and folder metadata (name, path, size, dates, MD5 checksum)
- Populates the Migration Database (`Directories`, `ImportFiles`, `ActivityLog`)
- Input validation at startup to fails fast with clear messages
- Separate application logging to file
- Designed for extension to additional source systems

---

## Design Decisions

### Transformation in Python, not SQL

All data cleansing and normalisation happens in the Python pipeline before data is written to the database. The Migration DB is treated as a load target, not a transformation workspace. This keeps business logic version-controlled, testable, and source-agnostic.

Post-load SQL transformations (deduplication, folder restructuring, customer-specific adjustments) remain available to migration teams as a separate manual step — but are outside the scope of this tool.

File path length validation is excluded as this is a post-load concern that can be handled within the database once the PDM Vault root path is known.

### Error Handling

Errors are classified as recoverable or unrecoverable. Recoverable errors (locked files, unreadable metadata) are logged and skipped — the pipeline continues. Unrecoverable errors (DB connection failure, invalid root path) halt the pipeline immediately with a clear message.

### Idempotency

v1 validates that the `ImportFiles` table is empty before running. The pipeline will not proceed if records exist, preventing accidental duplication. The user must explicitly clear the table before re-running.

### Batch Inserts

Data is written to the database in batches, not row-by-row. Batch size is configurable. This is standard practice for pipeline performance at scale.

### Testing

Transformer logic is unit tested with `pytest`. Tests require no database connection or live file system — inputs and expected outputs are defined in the test itself. Integration and end-to-end testing is planned for a future version.

---

## Known Limitations (v1)

- Windows file system source only. Additional sources (SolidWorks PDM, Windchill) planned.
- SA authentication only. Windows Authentication planned.
- No resume capability. If the pipeline fails mid-run, the DB should be cleared before restarting.
- SolidWorks custom properties and cross-references not yet extracted. Requires SolidWorks Document Manager SDK (planned).
- No GUI. Command line only.
- Concurrent pipeline instances are not supported.
- Directories.Path is not populated by the harvester and that RebuildDirectoryPaths should be run post-load if needed.

---

## Requirements

- Python 3.14+
- SQL Server 2019+
- SolidWorks PDM Migration Database (schema: `CreateMigrationDB.sql`)

### Python Dependencies

```
pyodbc
pathlib
hashlib
python-dotenv
pytest
```

Install with:

```bash
pip install -r requirements.txt
```

---

## Usage

```bash
python main.py --root "C:\path\to\files" --server SQLINSTANCE --database Migration --password yourpassword
```

---

## Project Structure

```
pdm-harvester/
├── main.py                  # Entry point
├── config.py                # Connection settings, user inputs
├── db/
│   ├── connection.py        # SQL Server connection handler
│   └── repository.py        # DB write functions
├── sources/
│   ├── base.py              # Abstract base class for all sources
│   └── windows_fs/
│       ├── scanner.py       # File system walker
│       └── metadata.py      # Metadata extraction
├── pipeline/
│   ├── runner.py            # Pipeline orchestration
│   └── transformer.py       # Cleansing and normalisation
├── utils/
│   ├── id_generator.py      # DocumentID and ProjectID generation
│   └── logger.py            # Application logging
├── tests/
│   └── dummy_data/          # Dummy folder structure for testing
├── requirements.txt
└── README.md
```

---

## Roadmap

- [ ] v1 — Windows file system harvest (this version)
- [ ] v2 — SolidWorks Document Manager integration (custom properties, cross-references)
- [ ] v2 — Windows Authentication
- [ ] v2 — Incremental/delta load support
- [ ] v3 — Additional source systems (SolidWorks PDM, Windchill)
- [ ] v3 — GUI

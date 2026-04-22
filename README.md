# CS3960 Final Project — TardisDB-style MySQL Shell

Project based off of TardisDB:
<https://dl.acm.org/doi/10.1145/3448016.3452767>

## Prerequisites

### Python 3

**macOS (using Homebrew):**
```bash
brew install python
```

If you don't have Homebrew, install it from <https://brew.sh/>. Alternatively, download from <https://www.python.org/downloads/macos/>.

**Windows (using Chocolatey):**
```bash
choco install python
```

Alternatively, download from <https://www.python.org/downloads/windows/> and make sure to check "Add Python to PATH" during installation.

Verify the install:
```bash
python3 --version
```

### MySQL Server

**macOS (using Homebrew):**
```bash
brew install mysql
brew services start mysql
```

**Windows (using Chocolatey):**
```bash
choco install mysql
# Then start the MySQL service via Services app or Command Prompt:
net start MySQL80
```

Alternatively, download MySQL Community Server from <https://dev.mysql.com/downloads/mysql/> and follow the installer.

### Python Dependencies

```bash
pip install mysql-connector-python
```

### Set up Database

After MySQL is running, log in and create the database:

```bash
mysql -u root -h 127.0.0.1 -p
```

Then at the `mysql>` prompt:

```sql
CREATE DATABASE CS3960_project;
EXIT;
```

Now load the schema and views into the database:

```bash
mysql -u root -h 127.0.0.1 -p CS3960_project < schema.sql
mysql -u root -h 127.0.0.1 -p CS3960_project < views.sql
```

## Run

```bash
python3 tardis_shell.py --database YOUR_DB [--user USER] [--host HOST] [-v]
```

Example:

```bash
python3 tardis_shell.py --host 127.0.0.1 --user root --database CS3960_project -v
```

## Supported commands

All statements end with `;` except the dot commands.

### Branch management

```sql
CREATE BRANCH <name> FROM <parent>;
DELETE BRANCH <name>;
SHOW BRANCHES;
USE BRANCH <name>;                 -- sets default branch for this session
```

### Table management

```sql
CREATE VERSIONED TABLE <name> (col TYPE, ...);
                                   -- adds tuple_id/branch_id/created/is_deleted
                                   -- and auto-creates a <name>_visible view

CREATE TABLE <name> (...);         -- plain passthrough (non-versioned table)

DROP TABLE [IF EXISTS] <name>;     -- drops table + its _visible view if versioned

ALTER TABLE <name> <action>, ...;  -- for versioned tables, runs the ALTER
                                   -- and then rebuilds the _visible view and
                                   -- the shell's column metadata.
                                   -- For non-versioned tables, plain passthrough.
```

**Restrictions on `ALTER TABLE` for versioned tables:**

- `tuple_id`, `branch_id`, `created`, and `is_deleted` cannot be dropped or
  renamed (`MODIFY` to change their type is allowed).
- No other column may be renamed *to* one of those reserved names.
- The table itself cannot be `RENAME`d.

**Examples:**

```sql
ALTER TABLE employees ADD COLUMN email VARCHAR(128);
ALTER TABLE employees ADD COLUMN title VARCHAR(64) AFTER name;
ALTER TABLE employees DROP COLUMN title;
ALTER TABLE employees MODIFY COLUMN salary DECIMAL(12,2);
ALTER TABLE employees RENAME COLUMN email TO work_email;
```

### Data manipulation

```sql
SELECT ... FROM <table> [VERSION <branch>] [JOIN ... VERSION ...] ...;
INSERT INTO <table> [VERSION <branch>] [(cols)] VALUES (...);
UPDATE <table> [VERSION <branch>] SET col=val, ... [WHERE ...];
DELETE FROM <table> [VERSION <branch>] [WHERE ...];
```

### Dot commands

| Command     | Description                                  |
| ----------- | -------------------------------------------- |
| `.help`     | Show this help                               |
| `.verbose`  | Toggle printing of the translated SQL        |
| `.tables`   | List versioned tables known to this session  |
| `.quit`     | Exit                                         |

### disclaimer: some of the tardis_shell.py and views.sql code was created with the assistance of generative AI.

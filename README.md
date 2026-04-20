Project based off of tardisdb from:
https://dl.acm.org/doi/10.1145/3448016.3452767


Prerequisites:
    pip install mysql-connector-python
    Run schema.sql and views.sql against your database first.

Run:
    python3 tardis_shell.py --database YOUR_DB [--user USER] [--host HOST] [-v]

Example:
    python3 tardis_shell.py --host 127.0.0.1 --user root --database CS3960_project -v

Supported commands (all end with ; except dot commands):

  CREATE BRANCH <name> FROM <parent>;
  DELETE BRANCH <name>;
  SHOW BRANCHES;
  USE BRANCH <name>;                 -- sets default branch for this session

  CREATE VERSIONED TABLE <name> (col TYPE, ...);
                                     -- adds tuple_id/branch_id/created/is_deleted
                                     -- and auto-creates a <name>_visible view
  CREATE TABLE <name> (...);         -- plain passthrough (non-versioned table)
  DROP TABLE [IF EXISTS] <name>;     -- drops table + its _visible view if versioned
  ALTER TABLE <name> <action>, ...;  -- for versioned tables, runs the ALTER
                                     -- and then rebuilds the _visible view and
                                     -- the shell's column metadata.
                                     -- For non-versioned tables, plain passthrough.
                                     --
                                     -- Restrictions on versioned tables:
                                     --   * tuple_id, branch_id, created, is_deleted
                                     --     cannot be dropped or renamed
                                     --     (MODIFY to change their type is allowed)
                                     --   * no other column may be renamed TO one of
                                     --     those reserved names
                                     --   * the table itself cannot be RENAMEd
                                     --
                                     -- Examples:
                                     --   ALTER TABLE employees ADD COLUMN email VARCHAR(128);
                                     --   ALTER TABLE employees ADD COLUMN title VARCHAR(64) AFTER name;
                                     --   ALTER TABLE employees DROP COLUMN title;
                                     --   ALTER TABLE employees MODIFY COLUMN salary DECIMAL(12,2);
                                     --   ALTER TABLE employees RENAME COLUMN email TO work_email;

  SELECT ... FROM <table> [VERSION <branch>] [JOIN ... VERSION ...] ...;
  INSERT INTO <table> [VERSION <branch>] [(cols)] VALUES (...);
  UPDATE <table> [VERSION <branch>] SET col=val, ... [WHERE ...];
  DELETE FROM <table> [VERSION <branch>] [WHERE ...];

  .help          show this help
  .verbose       toggle printing of the translated SQL
  .tables        list versioned tables known to this session
  .quit          exit
"""

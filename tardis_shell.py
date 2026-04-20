#!/usr/bin/env python3
"""
tardis_shell.py - TardisDB-style shell for the versioned MySQL schema.

Translates TardisDB's proposed SQL extensions (CREATE BRANCH, VERSION keyword,
DELETE BRANCH) into plain MySQL operations against a schema where each
versioned table has (tuple_id, branch_id, created) as its composite PK and an
is_deleted flag acting as a tombstone.

Prerequisites:
    pip install mysql-connector-python
    Run schema.sql and views.sql against your database first.

Run:
    python tardis_shell.py --database YOUR_DB [--user USER] [--host HOST] [-v]

Supported commands (all end with ; except dot commands):

  CREATE BRANCH <name> FROM <parent>;
  DELETE BRANCH <name>;
  SHOW BRANCHES;
  USE BRANCH <name>;                    -- sets default branch for this session

  SELECT ... FROM <table> [VERSION <branch>] [JOIN ... VERSION ...] ...;
  INSERT INTO <table> [VERSION <branch>] [(cols)] VALUES (...);
  UPDATE <table> [VERSION <branch>] SET col=val, ... [WHERE ...];
  DELETE FROM <table> [VERSION <branch>] [WHERE ...];

  .help          show this help
  .verbose       toggle printing of the translated SQL
  .quit          exit
"""

import argparse
import getpass
import re
import sys

try:
    import mysql.connector
except ImportError:
    print("mysql-connector-python is required.  Install with:")
    print("    pip install mysql-connector-python")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Schema metadata — tells the shell which columns belong to each versioned
# table, which ones the user supplies (vs. which we inject like branch_id),
# and which _visible view to query for reads.
# ---------------------------------------------------------------------------
VERSIONED_TABLES = {
    'employees': {
        'full_columns': [
            'tuple_id', 'name', 'salary', 'joined_on', 'department_tuple_id',
            'branch_id', 'created', 'is_deleted',
        ],
        'user_columns': [
            'tuple_id', 'name', 'salary', 'joined_on', 'department_tuple_id',
        ],
        'visible_view': 'employees_visible',
    },
    'departments': {
        'full_columns': [
            'tuple_id', 'name', 'manager_tuple_id', 'budget',
            'branch_id', 'created', 'is_deleted',
        ],
        'user_columns': [
            'tuple_id', 'name', 'manager_tuple_id', 'budget',
        ],
        'visible_view': 'departments_visible',
    },
    'paystubs': {
        'full_columns': [
            'tuple_id', 'employee_tuple_id', 'pay_period_start', 'pay_period_end',
            'gross_amount', 'net_amount', 'issued_on',
            'branch_id', 'created', 'is_deleted',
        ],
        'user_columns': [
            'tuple_id', 'employee_tuple_id', 'pay_period_start', 'pay_period_end',
            'gross_amount', 'net_amount', 'issued_on',
        ],
        'visible_view': 'paystubs_visible',
    },
}


# Regex for `<table> VERSION <branch>` — used to find cross-branch references
VERSION_CLAUSE_RE = re.compile(
    r'\b(' + '|'.join(VERSIONED_TABLES.keys()) + r')\s+VERSION\s+(\w+)\b',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Shell
# ---------------------------------------------------------------------------
class TardisShell:
    def __init__(self, conn, verbose=False):
        self.conn = conn
        self.current_branch = 'master'
        self.verbose = verbose

    # -- small helpers ------------------------------------------------------

    def _log(self, sql, params=None):
        if not self.verbose:
            return
        print(f"    [sql] {sql}")
        if params:
            print(f"    [prm] {params}")

    def _branch_id(self, name):
        cur = self.conn.cursor()
        cur.execute("SELECT branch_id FROM branches WHERE branch_name = %s", (name,))
        row = cur.fetchone()
        cur.close()
        if row is None:
            raise ValueError(f"Branch '{name}' not found")
        return row[0]

    def _set_branch_var(self, branch_name):
        bid = self._branch_id(branch_name)
        cur = self.conn.cursor()
        cur.execute("SET @current_branch = %s", (bid,))
        cur.close()
        self._log(f"SET @current_branch = {bid};  -- '{branch_name}'")
        return bid

    def _print_table(self, columns, rows):
        if not rows:
            print("    (no rows)")
            return
        widths = [len(str(c)) for c in columns]
        stringified = []
        for row in rows:
            s = ['NULL' if v is None else str(v) for v in row]
            stringified.append(s)
            for i, cell in enumerate(s):
                widths[i] = max(widths[i], len(cell))

        def line(cells):
            return " | ".join(c.ljust(widths[i]) for i, c in enumerate(cells))

        print("    " + line([str(c) for c in columns]))
        print("    " + "-+-".join("-" * w for w in widths))
        for s in stringified:
            print("    " + line(s))
        print(f"    ({len(rows)} row{'s' if len(rows) != 1 else ''})")

    @staticmethod
    def _split_top_level(s):
        """Split on commas that are outside quotes and parentheses."""
        out, buf, in_quote, depth = [], [], False, 0
        i = 0
        while i < len(s):
            ch = s[i]
            if ch == "'" and (i == 0 or s[i - 1] != '\\'):
                in_quote = not in_quote
                buf.append(ch)
            elif not in_quote and ch == '(':
                depth += 1
                buf.append(ch)
            elif not in_quote and ch == ')':
                depth -= 1
                buf.append(ch)
            elif not in_quote and depth == 0 and ch == ',':
                out.append(''.join(buf).strip())
                buf = []
            else:
                buf.append(ch)
            i += 1
        if buf:
            out.append(''.join(buf).strip())
        return out

    # -- command dispatch ---------------------------------------------------

    PATTERNS = [
        (r'^\s*\.(\w+)\s*(.*)$',                        'do_dot'),
        (r'^\s*CREATE\s+BRANCH\s+(\w+)\s+FROM\s+(\w+)\s*$',     'do_create_branch'),
        (r'^\s*DELETE\s+BRANCH\s+(\w+)\s*$',            'do_delete_branch'),
        (r'^\s*SHOW\s+BRANCHES\s*$',                    'do_show_branches'),
        (r'^\s*USE\s+BRANCH\s+(\w+)\s*$',               'do_use_branch'),
        (r'^\s*SELECT\b',                               'do_select'),
        (r'^\s*INSERT\s+INTO\b',                        'do_insert'),
        (r'^\s*UPDATE\b',                               'do_update'),
        (r'^\s*DELETE\s+FROM\b',                        'do_delete'),
    ]

    def execute(self, sql):
        sql = sql.strip().rstrip(';').strip()
        if not sql:
            return
        for pattern, handler in self.PATTERNS:
            m = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
            if m:
                getattr(self, handler)(sql, m)
                return
        print("    Unrecognized command. Type .help for usage.")

    # -- dot commands -------------------------------------------------------

    def do_dot(self, sql, m):
        cmd = m.group(1).lower()
        if cmd in ('quit', 'exit'):
            raise SystemExit(0)
        if cmd == 'verbose':
            self.verbose = not self.verbose
            print(f"    verbose: {'ON' if self.verbose else 'OFF'}")
        elif cmd == 'help':
            print(__doc__)
        else:
            print(f"    Unknown: .{cmd}")

    # -- branch management --------------------------------------------------

    def do_create_branch(self, sql, m):
        new_name, parent_name = m.group(1), m.group(2)
        cur = self.conn.cursor()
        try:
            self._log("CALL create_branch(%s, %s);", (new_name, parent_name))
            cur.execute("CALL create_branch(%s, %s)", (new_name, parent_name))
            rows = cur.fetchall()
            if rows:
                cols = [d[0] for d in cur.description]
                self._print_table(cols, rows)
            while cur.nextset():
                pass
            self.conn.commit()
            print(f"    Branch '{new_name}' forked from '{parent_name}'")
        finally:
            cur.close()

    def do_delete_branch(self, sql, m):
        name = m.group(1)
        cur = self.conn.cursor()
        try:
            self._log("CALL delete_branch(%s);", (name,))
            cur.execute("CALL delete_branch(%s)", (name,))
            while cur.nextset():
                pass
            self.conn.commit()
            print(f"    Branch '{name}' deleted")
            if self.current_branch == name:
                self.current_branch = 'master'
                print("    (default branch reset to 'master')")
        finally:
            cur.close()

    def do_show_branches(self, sql, m):
        q = """
            SELECT b.branch_id, b.branch_name, p.branch_name AS parent, b.created_at
            FROM branches b
            LEFT JOIN branches p ON b.parent_id = p.branch_id
            ORDER BY b.branch_id
        """
        self._log(q.strip())
        cur = self.conn.cursor()
        cur.execute(q)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
        self._print_table(cols, rows)

    def do_use_branch(self, sql, m):
        name = m.group(1)
        self._branch_id(name)  # raises if not found
        self.current_branch = name
        print(f"    Default branch: {name}")

    # -- SELECT -------------------------------------------------------------

    def do_select(self, sql, m):
        refs = VERSION_CLAUSE_RE.findall(sql)
        # refs is list of (table, branch)
        distinct_branches = {b for _, b in refs}

        if len(distinct_branches) <= 1:
            # Zero or one branch mentioned — use visible views with one SET
            branch = next(iter(distinct_branches)) if distinct_branches else self.current_branch
            rewritten = self._rewrite_version_to_views(sql)
            rewritten = self._rewrite_bare_tables_to_views(rewritten)
            self._set_branch_var(branch)
            self._log(rewritten)
            self._run_and_print(rewritten)
        else:
            # Multiple branches — materialize each (table, branch) into a temp
            self._run_multibranch_select(sql)

    def _rewrite_version_to_views(self, sql):
        def repl(m):
            table = m.group(1).lower()
            return VERSIONED_TABLES[table]['visible_view']
        return VERSION_CLAUSE_RE.sub(repl, sql)

    def _rewrite_bare_tables_to_views(self, sql):
        """For bare `FROM employees` / `JOIN departments` (no VERSION clause),
        point at the _visible view so the current branch context applies."""
        table_alt = '|'.join(VERSIONED_TABLES.keys())
        # Don't rewrite if it's already the _visible view or a temp table
        for kw in ('FROM', 'JOIN'):
            pat = re.compile(
                rf'\b({kw})\s+({table_alt})\b(?!\s*_)', re.IGNORECASE
            )

            def repl(m):
                return f"{m.group(1)} {VERSIONED_TABLES[m.group(2).lower()]['visible_view']}"

            sql = pat.sub(repl, sql)
        return sql

    def _run_multibranch_select(self, sql):
        """Materialize each distinct (table, branch) pair into a TEMP TABLE,
        then substitute temp-table names for the VERSION clauses."""
        cur = self.conn.cursor()
        temp_map = {}  # (table, branch) -> temp_name
        try:
            # Walk all matches and materialize each unique pair
            for m in VERSION_CLAUSE_RE.finditer(sql):
                table, branch = m.group(1).lower(), m.group(2)
                key = (table, branch)
                if key in temp_map:
                    continue
                temp_name = f"_tmp_{table}_{branch}"
                temp_map[key] = temp_name
                bid = self._branch_id(branch)
                cur.execute("SET @current_branch = %s", (bid,))
                view = VERSIONED_TABLES[table]['visible_view']
                cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {temp_name}")
                cur.execute(f"CREATE TEMPORARY TABLE {temp_name} AS SELECT * FROM {view}")
                self._log(
                    f"-- materialize {table} VERSION {branch}  ->  {temp_name}"
                )

            # Substitute VERSION clauses with temp table names (right-to-left
            # so earlier offsets remain valid)
            matches = list(VERSION_CLAUSE_RE.finditer(sql))
            new_sql = sql
            for mt in reversed(matches):
                table, branch = mt.group(1).lower(), mt.group(2)
                new_sql = new_sql[:mt.start()] + temp_map[(table, branch)] + new_sql[mt.end():]

            self._log(new_sql)
            cur.execute(new_sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            self._print_table(cols, rows)
        finally:
            for tn in temp_map.values():
                try:
                    cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tn}")
                except Exception:
                    pass
            cur.close()

    def _run_and_print(self, sql):
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            self._print_table(cols, rows)
        finally:
            cur.close()

    # -- INSERT -------------------------------------------------------------

    INSERT_RE = re.compile(
        r'^INSERT\s+INTO\s+(\w+)(?:\s+VERSION\s+(\w+))?\s*'
        r'(?:\(([^)]+)\))?\s*VALUES\s*\((.+)\)\s*$',
        re.IGNORECASE | re.DOTALL,
    )

    def do_insert(self, sql, m_outer):
        m = self.INSERT_RE.match(sql)
        if not m:
            print("    Could not parse INSERT.")
            return
        table = m.group(1).lower()
        branch = m.group(2) or self.current_branch
        col_list = m.group(3)
        val_list = m.group(4)

        if table not in VERSIONED_TABLES:
            print(f"    '{table}' is not a versioned table")
            return
        meta = VERSIONED_TABLES[table]

        user_cols = ([c.strip() for c in col_list.split(',')]
                     if col_list else list(meta['user_columns']))
        user_vals = self._split_top_level(val_list)

        if len(user_cols) != len(user_vals):
            print(f"    column count {len(user_cols)} != value count {len(user_vals)}")
            return

        bid = self._branch_id(branch)
        provided = dict(zip([c.lower() for c in user_cols], user_vals))

        cols_out, vals_out = [], []
        for col in meta['full_columns']:
            if col == 'branch_id':
                cols_out.append(col); vals_out.append(str(bid))
            elif col == 'created':
                cols_out.append(col); vals_out.append('NOW(6)')
            elif col == 'is_deleted':
                cols_out.append(col); vals_out.append('FALSE')
            elif col in provided:
                cols_out.append(col); vals_out.append(provided[col])
            else:
                cols_out.append(col); vals_out.append('NULL')

        final = (f"INSERT INTO {table} ({', '.join(cols_out)}) "
                 f"VALUES ({', '.join(vals_out)})")
        self._log(final)

        cur = self.conn.cursor()
        try:
            cur.execute(final)
            self.conn.commit()
            print(f"    Inserted into {table} VERSION {branch}")
        finally:
            cur.close()

    # -- UPDATE -------------------------------------------------------------

    UPDATE_RE = re.compile(
        r'^UPDATE\s+(\w+)(?:\s+VERSION\s+(\w+))?\s+SET\s+(.+?)'
        r'(?:\s+WHERE\s+(.+))?$',
        re.IGNORECASE | re.DOTALL,
    )

    def do_update(self, sql, m_outer):
        m = self.UPDATE_RE.match(sql)
        if not m:
            print("    Could not parse UPDATE.")
            return
        table = m.group(1).lower()
        branch = m.group(2) or self.current_branch
        set_clause = m.group(3).strip()
        where = m.group(4)

        if table not in VERSIONED_TABLES:
            print(f"    '{table}' is not a versioned table")
            return
        meta = VERSIONED_TABLES[table]

        set_pairs = {}
        for pair in self._split_top_level(set_clause):
            eq = pair.index('=')
            set_pairs[pair[:eq].strip().lower()] = pair[eq + 1:].strip()

        bid = self._branch_id(branch)

        # Build INSERT ... SELECT from the visible view
        select_exprs = []
        for col in meta['full_columns']:
            if col == 'branch_id':
                select_exprs.append(str(bid))
            elif col == 'created':
                select_exprs.append('NOW(6)')
            elif col == 'is_deleted':
                select_exprs.append('FALSE')
            elif col in set_pairs:
                select_exprs.append(set_pairs[col])
            else:
                select_exprs.append(col)

        self._set_branch_var(branch)
        where_sql = f" WHERE {where}" if where else ""
        final = (f"INSERT INTO {table} ({', '.join(meta['full_columns'])}) "
                 f"SELECT {', '.join(select_exprs)} "
                 f"FROM {meta['visible_view']}{where_sql}")
        self._log(final)

        cur = self.conn.cursor()
        try:
            cur.execute(final)
            count = cur.rowcount
            self.conn.commit()
            print(f"    Updated {count} row(s) in {table} VERSION {branch}")
        finally:
            cur.close()

    # -- DELETE -------------------------------------------------------------

    DELETE_RE = re.compile(
        r'^DELETE\s+FROM\s+(\w+)(?:\s+VERSION\s+(\w+))?'
        r'(?:\s+WHERE\s+(.+))?$',
        re.IGNORECASE | re.DOTALL,
    )

    def do_delete(self, sql, m_outer):
        m = self.DELETE_RE.match(sql)
        if not m:
            print("    Could not parse DELETE.")
            return
        table = m.group(1).lower()
        branch = m.group(2) or self.current_branch
        where = m.group(3)

        if table not in VERSIONED_TABLES:
            print(f"    '{table}' is not a versioned table")
            return
        meta = VERSIONED_TABLES[table]
        bid = self._branch_id(branch)

        # Insert a tombstone row per matching visible row
        select_exprs = []
        for col in meta['full_columns']:
            if col == 'branch_id':
                select_exprs.append(str(bid))
            elif col == 'created':
                select_exprs.append('NOW(6)')
            elif col == 'is_deleted':
                select_exprs.append('TRUE')
            else:
                select_exprs.append(col)

        self._set_branch_var(branch)
        where_sql = f" WHERE {where}" if where else ""
        final = (f"INSERT INTO {table} ({', '.join(meta['full_columns'])}) "
                 f"SELECT {', '.join(select_exprs)} "
                 f"FROM {meta['visible_view']}{where_sql}")
        self._log(final)

        cur = self.conn.cursor()
        try:
            cur.execute(final)
            count = cur.rowcount
            self.conn.commit()
            print(f"    Tombstoned {count} row(s) in {table} VERSION {branch}")
        finally:
            cur.close()


# ---------------------------------------------------------------------------
# REPL entry point
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description='TardisDB-style MySQL shell')
    ap.add_argument('--host',     default='localhost')
    ap.add_argument('--user',     default='root')
    ap.add_argument('--password', default=None)
    ap.add_argument('--database', required=True)
    ap.add_argument('--verbose', '-v', action='store_true',
                    help='print translated SQL before executing')
    args = ap.parse_args()

    pw = args.password
    if pw is None:
        pw = getpass.getpass('MySQL password: ')

    try:
        conn = mysql.connector.connect(
            host=args.host, user=args.user, password=pw,
            database=args.database, autocommit=False,
        )
    except mysql.connector.Error as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    shell = TardisShell(conn, verbose=args.verbose)
    print("TardisDB-style MySQL shell.  .help for commands, .quit to exit.")
    print(f"Default branch: {shell.current_branch}   "
          f"(verbose: {'ON' if shell.verbose else 'OFF'})")

    buf = []
    try:
        while True:
            prompt = (f"tardis[{shell.current_branch}]> " if not buf
                      else "        ...> ")
            try:
                line = input(prompt)
            except EOFError:
                print()
                break

            stripped = line.strip()
            if not stripped and not buf:
                continue

            # Dot commands are single-line and don't need a terminator
            if not buf and stripped.startswith('.'):
                try:
                    shell.execute(stripped)
                except SystemExit:
                    raise
                except Exception as e:
                    print(f"    error: {type(e).__name__}: {e}")
                continue

            buf.append(line)
            if line.rstrip().endswith(';'):
                full = ' '.join(buf)
                buf = []
                try:
                    shell.execute(full)
                except SystemExit:
                    raise
                except mysql.connector.Error as e:
                    print(f"    MySQL error: {e}")
                except Exception as e:
                    print(f"    error: {type(e).__name__}: {e}")
    except (KeyboardInterrupt, SystemExit):
        print()
    finally:
        conn.close()
        print("Goodbye.")


if __name__ == '__main__':
    main()

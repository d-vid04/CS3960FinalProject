-- ============================================================
-- views.sql
-- TardisDB-style versioning helpers for a plain MySQL schema.
--
-- Assumes base tables (schema.sql) are already created:
--   branches, employees, departments, paystubs
--
-- Safe to re-run: all statements are idempotent.
--
-- Run with:
--   mysql -u USER -p DATABASE < views.sql
-- ============================================================


-- ------------------------------------------------------------
-- 0. Tear down in reverse dependency order so the script is re-runnable
-- ------------------------------------------------------------
-- Views first (they depend on the function), then the function.
DROP VIEW      IF EXISTS employees_visible;
DROP VIEW      IF EXISTS departments_visible;
DROP VIEW      IF EXISTS paystubs_visible;
DROP VIEW      IF EXISTS branch_lineage;
DROP FUNCTION  IF EXISTS current_branch;
DROP PROCEDURE IF EXISTS create_branch;
DROP PROCEDURE IF EXISTS delete_branch;


-- ------------------------------------------------------------
-- 1. Add branch_name column (idempotent — MySQL has no IF NOT EXISTS
--    for ADD COLUMN, so we check information_schema first)
-- ------------------------------------------------------------
SET @col_exists := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME   = 'branches'
      AND COLUMN_NAME  = 'branch_name'
);

SET @sql := IF(@col_exists = 0,
    'ALTER TABLE branches ADD COLUMN branch_name VARCHAR(64) UNIQUE',
    'DO 0'  -- no-op
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Ensure master branch exists and is named
INSERT INTO branches (branch_id, parent_id, branch_name, created_at)
VALUES (1, NULL, 'master', NOW(6))
ON DUPLICATE KEY UPDATE
    branch_name = COALESCE(branch_name, 'master');


-- ------------------------------------------------------------
-- 2. current_branch() helper function
-- ------------------------------------------------------------
-- MySQL does not allow user variables (@foo) in VIEW definitions
-- (error 1351). Wrapping the read in a function bypasses the check
-- because the parser does not look inside function bodies.

DELIMITER //
CREATE FUNCTION current_branch()
    RETURNS INT UNSIGNED
    NOT DETERMINISTIC
    READS SQL DATA
BEGIN
    RETURN @current_branch;
END //
DELIMITER ;


-- ------------------------------------------------------------
-- 3. Branch lineage view
-- ------------------------------------------------------------
-- Walks from current_branch() up the ancestry chain. For each branch
-- in the chain, fork_cutoff tells us which versions are visible:
-- a tuple created in that ancestor branch is visible only if
-- tuple.created < fork_cutoff. For the target branch itself, NULL.
CREATE VIEW branch_lineage AS
WITH RECURSIVE lineage (branch_id, parent_id, created_at, fork_cutoff) AS (
    SELECT branch_id, parent_id, created_at,
           CAST(NULL AS DATETIME(6))
    FROM branches
    WHERE branch_id = current_branch()

    UNION ALL

    SELECT p.branch_id, p.parent_id, p.created_at, l.created_at
    FROM lineage l
    JOIN branches p ON p.branch_id = l.parent_id
)
SELECT branch_id, fork_cutoff FROM lineage;


-- ------------------------------------------------------------
-- 4. Per-table visibility views
-- ------------------------------------------------------------

CREATE VIEW employees_visible AS
SELECT tuple_id, name, salary, joined_on, department_tuple_id,
       branch_id AS _source_branch, created AS _version_ts
FROM (
    SELECT e.*,
           ROW_NUMBER() OVER (
               PARTITION BY e.tuple_id
               ORDER BY e.created DESC
           ) AS _rn
    FROM employees e
    JOIN branch_lineage l ON e.branch_id = l.branch_id
    WHERE l.fork_cutoff IS NULL OR e.created < l.fork_cutoff
) ranked
WHERE _rn = 1 AND is_deleted = FALSE;


CREATE VIEW departments_visible AS
SELECT tuple_id, name, manager_tuple_id, budget,
       branch_id AS _source_branch, created AS _version_ts
FROM (
    SELECT d.*,
           ROW_NUMBER() OVER (
               PARTITION BY d.tuple_id
               ORDER BY d.created DESC
           ) AS _rn
    FROM departments d
    JOIN branch_lineage l ON d.branch_id = l.branch_id
    WHERE l.fork_cutoff IS NULL OR d.created < l.fork_cutoff
) ranked
WHERE _rn = 1 AND is_deleted = FALSE;


CREATE VIEW paystubs_visible AS
SELECT tuple_id, employee_tuple_id, pay_period_start, pay_period_end,
       gross_amount, net_amount, issued_on,
       branch_id AS _source_branch, created AS _version_ts
FROM (
    SELECT p.*,
           ROW_NUMBER() OVER (
               PARTITION BY p.tuple_id
               ORDER BY p.created DESC
           ) AS _rn
    FROM paystubs p
    JOIN branch_lineage l ON p.branch_id = l.branch_id
    WHERE l.fork_cutoff IS NULL OR p.created < l.fork_cutoff
) ranked
WHERE _rn = 1 AND is_deleted = FALSE;


-- ------------------------------------------------------------
-- 5. Branch management stored procedures
-- ------------------------------------------------------------

DELIMITER //

CREATE PROCEDURE create_branch(
    IN p_new_name    VARCHAR(64),
    IN p_parent_name VARCHAR(64)
)
BEGIN
    DECLARE v_parent_id INT UNSIGNED;
    DECLARE v_new_id    INT UNSIGNED;

    SELECT branch_id INTO v_parent_id
    FROM branches WHERE branch_name = p_parent_name;

    IF v_parent_id IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Parent branch not found';
    END IF;

    IF EXISTS (SELECT 1 FROM branches WHERE branch_name = p_new_name) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Branch name already exists';
    END IF;

    SELECT COALESCE(MAX(branch_id), 0) + 1 INTO v_new_id FROM branches;

    INSERT INTO branches (branch_id, parent_id, branch_name, created_at)
    VALUES (v_new_id, v_parent_id, p_new_name, NOW(6));

    SELECT v_new_id      AS new_branch_id,
           p_new_name    AS branch_name,
           p_parent_name AS forked_from;
END //

CREATE PROCEDURE delete_branch(IN p_name VARCHAR(64))
BEGIN
    DECLARE v_branch_id INT UNSIGNED;
    DECLARE v_done      INT DEFAULT FALSE;
    DECLARE v_table     VARCHAR(64);

    -- Cursor over every table that has a FK to branches(branch_id),
    -- so dynamically-created versioned tables are cleaned up too.
    DECLARE fk_cur CURSOR FOR
        SELECT DISTINCT TABLE_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND REFERENCED_TABLE_NAME = 'branches'
          AND REFERENCED_COLUMN_NAME = 'branch_id'
          AND TABLE_NAME != 'branches';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;

    SELECT branch_id INTO v_branch_id
    FROM branches WHERE branch_name = p_name;

    IF v_branch_id IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Branch not found';
    END IF;

    IF p_name = 'master' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Cannot delete master branch';
    END IF;

    IF EXISTS (SELECT 1 FROM branches WHERE parent_id = v_branch_id) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Branch has child branches; delete those first';
    END IF;

    OPEN fk_cur;
    purge_loop: LOOP
        FETCH fk_cur INTO v_table;
        IF v_done THEN
            LEAVE purge_loop;
        END IF;
        SET @sql := CONCAT(
            'DELETE FROM `', v_table, '` WHERE branch_id = ', v_branch_id
        );
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE fk_cur;

    DELETE FROM branches WHERE branch_id = v_branch_id;
END //

DELIMITER ;

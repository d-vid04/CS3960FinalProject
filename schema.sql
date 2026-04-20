
CREATE TABLE `branches` (
  `branch_id`  int unsigned NOT NULL,
  `parent_id`  int unsigned DEFAULT NULL,
  `created_at` datetime     NOT NULL,
  PRIMARY KEY (`branch_id`),
  KEY `parent_fk` (`parent_id`),
  CONSTRAINT `parent_fk` FOREIGN KEY (`parent_id`) REFERENCES `branches` (`branch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `employees` (
  `tuple_id`            int unsigned NOT NULL,
  `name`                varchar(256) DEFAULT NULL,
  `salary`              int unsigned DEFAULT NULL,
  `joined_on`           date         DEFAULT NULL,
  `branch_id`           int unsigned NOT NULL,
  `created`             datetime     NOT NULL,
  `department_tuple_id` int unsigned DEFAULT NULL,
  `is_deleted`          tinyint(1)   NOT NULL DEFAULT '0',
  PRIMARY KEY (`tuple_id`, `branch_id`, `created`),
  KEY `branch_fk` (`branch_id`),
  CONSTRAINT `branch_fk` FOREIGN KEY (`branch_id`) REFERENCES `branches` (`branch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `departments` (
  `tuple_id`         int unsigned NOT NULL,
  `name`             varchar(256) DEFAULT NULL,
  `manager_tuple_id` int unsigned DEFAULT NULL,
  `budget`           int unsigned DEFAULT NULL,
  `branch_id`        int unsigned NOT NULL,
  `created`          datetime     NOT NULL,
  `is_deleted`       tinyint(1)   NOT NULL DEFAULT '0',
  PRIMARY KEY (`tuple_id`, `branch_id`, `created`),
  KEY `dept_branch_fk` (`branch_id`),
  CONSTRAINT `dept_branch_fk` FOREIGN KEY (`branch_id`) REFERENCES `branches` (`branch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `paystubs` (
  `tuple_id`          int unsigned NOT NULL,
  `employee_tuple_id` int unsigned NOT NULL,
  `pay_period_start`  date         NOT NULL,
  `pay_period_end`    date         NOT NULL,
  `gross_amount`      int unsigned NOT NULL,
  `net_amount`        int unsigned NOT NULL,
  `issued_on`         date         NOT NULL,
  `branch_id`         int unsigned NOT NULL,
  `created`           datetime     NOT NULL,
  `is_deleted`        tinyint(1)   NOT NULL DEFAULT '0',
  PRIMARY KEY (`tuple_id`, `branch_id`, `created`),
  KEY `paystub_branch_fk` (`branch_id`),
  CONSTRAINT `paystub_branch_fk` FOREIGN KEY (`branch_id`) REFERENCES `branches` (`branch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
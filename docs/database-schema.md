# Raw SQL Database Schemas (Non-EF Core Setup)

If your application does not use Entity Framework Core (e.g. you are using Dapper, DbUp, FluentMigrator, or raw ADO.NET), you must create the Outbox and Inbox tables manually in your database.

This document contains the optimized SQL schemas for **PostgreSQL**, **Microsoft SQL Server**, and **MySQL/MariaDB**.

---

## 1. PostgreSQL Schema

PostgreSQL uses `UUID` for efficient IDs and supports partial indexes to keep query times minimal.

```sql
-- 1. Create Transactional Outbox Table
CREATE TABLE qkafka_outbox (
    id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255) NULL,
    payload TEXT NOT NULL,
    headers TEXT NOT NULL,          -- JSON string or text block
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    dispatched_at TIMESTAMP WITH TIME ZONE NULL,
    attempts INT DEFAULT 0 NOT NULL
);

-- Partial index to speed up fetching undispatched messages (hot path)
CREATE INDEX idx_qkafka_outbox_unpublished 
ON qkafka_outbox (created_at) 
WHERE dispatched_at IS NULL;


-- 2. Create Transactional Inbox Table (Idempotency Guard)
CREATE TABLE qkafka_inbox (
    message_id VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,    -- 'Processing', 'Processed', 'Failed'
    received_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE NULL,
    PRIMARY KEY (message_id, consumer_group)
);

-- Index to clean up old processed/expired messages periodically
CREATE INDEX idx_qkafka_inbox_cleanup 
ON qkafka_inbox (received_at);
```

---

## 2. Microsoft SQL Server Schema

SQL Server schema uses `UNIQUEIDENTIFIER` for outbox IDs and clustered indexes optimized for locking.

```sql
-- 1. Create Transactional Outbox Table
CREATE TABLE qkafka_outbox (
    id UNIQUEIDENTIFIER PRIMARY KEY,
    topic NVARCHAR(255) NOT NULL,
    partition_key NVARCHAR(255) NULL,
    payload NVARCHAR(MAX) NOT NULL,
    headers NVARCHAR(MAX) NOT NULL,
    created_at DATETIMEOFFSET NOT NULL,
    dispatched_at DATETIMEOFFSET NULL,
    attempts INT DEFAULT 0 NOT NULL
);

-- Non-clustered filtered index for unpublished messages (hot path)
CREATE NONCLUSTERED INDEX idx_qkafka_outbox_unpublished
ON qkafka_outbox (created_at)
WHERE dispatched_at IS NULL;


-- 2. Create Transactional Inbox Table
CREATE TABLE qkafka_inbox (
    message_id NVARCHAR(255) NOT NULL,
    consumer_group NVARCHAR(255) NOT NULL,
    status NVARCHAR(50) NOT NULL,    -- 'Processing', 'Processed', 'Failed'
    received_at DATETIMEOFFSET NOT NULL,
    completed_at DATETIMEOFFSET NULL,
    CONSTRAINT PK_qkafka_inbox PRIMARY KEY CLUSTERED (message_id, consumer_group)
);

-- Index for background cleanup worker
CREATE NONCLUSTERED INDEX idx_qkafka_inbox_cleanup
ON qkafka_inbox (received_at);
```

---

## 3. MySQL / MariaDB Schema

MySQL uses `VARCHAR(36)` (or binary representation) for UUIDs and standard indexing.

```sql
-- 1. Create Transactional Outbox Table
CREATE TABLE qkafka_outbox (
    id VARCHAR(36) PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255) NULL,
    payload LONGTEXT NOT NULL,
    headers TEXT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    dispatched_at DATETIME(6) NULL,
    attempts INT DEFAULT 0 NOT NULL
);

-- Standard index for unpublished messages
CREATE INDEX idx_qkafka_outbox_unpublished 
ON qkafka_outbox (dispatched_at, created_at);


-- 2. Create Transactional Inbox Table
CREATE TABLE qkafka_inbox (
    message_id VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,    -- 'Processing', 'Processed', 'Failed'
    received_at DATETIME(6) NOT NULL,
    completed_at DATETIME(6) NULL,
    PRIMARY KEY (message_id, consumer_group)
);

-- Index for cleanup
CREATE INDEX idx_qkafka_inbox_cleanup 
ON qkafka_inbox (received_at);
```

---

## 4. Best Practices for Background Outbox Polling

When querying the outbox table manually in microservices, we highly recommend utilizing **concurrency control features** of the target DB to avoid lock contention:

* **PostgreSQL:** Use `FOR UPDATE SKIP LOCKED` inside your select queries.
  ```sql
  SELECT * FROM qkafka_outbox 
  WHERE dispatched_at IS NULL 
  ORDER BY created_at ASC 
  LIMIT 100 
  FOR UPDATE SKIP LOCKED;
  ```
* **SQL Server:** Use `WITH (UPDLOCK, READPAST)` query hints.
  ```sql
  SELECT TOP 100 * FROM qkafka_outbox WITH (UPDLOCK, READPAST)
  WHERE dispatched_at IS NULL
  ORDER BY created_at ASC;
  ```
* **MySQL 8.0+:** Supports `FOR UPDATE SKIP LOCKED`.
  ```sql
  SELECT * FROM qkafka_outbox 
  WHERE dispatched_at IS NULL 
  ORDER BY created_at ASC 
  LIMIT 100 
  FOR UPDATE SKIP LOCKED;
  ```

# This is a called program.  It is called from dbt_pscs_loader.py in create_fill_update_scd2_table function.

import datetime

def generate_alter_and_backup_sql(target_schema, target_table, staging_schema, staging_table, hashdiff_column, hashdiff_columns_str, scd_col_check_ddl):
    """
    Generates SQL to check for missing columns in the target table against a provided full DDL string
    (scd_col_check_ddl). It prepares ALTER TABLE statements for any missing columns found.
    """
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_table = f"{target_table}_backup_{timestamp}"

    # Escape single quotes in the DDL string for safe inclusion in the SQL string literal
    scd_ddl_sql_string = scd_col_check_ddl.replace("'", "''")

    return f"""
    -- Declare table variables 
    DECLARE @target_table NVARCHAR(128) = '{target_schema}.{target_table}';
    DECLARE @staging_table NVARCHAR(128) = '{staging_schema}.{staging_table}';
    
    -- Full DDL definition of the expected SCD columns
    DECLARE @expected_ddl NVARCHAR(MAX) = '{scd_ddl_sql_string}'; 
    DECLARE @columns_mismatch BIT = 0;
    
    -- Variables for DDL generation (INCLUDING THE MISSING @alter_sql)
    DECLARE @sql_exec NVARCHAR(MAX) = '';
    DECLARE @sql_log NVARCHAR(MAX) = '';
    DECLARE @column_definition NVARCHAR(MAX);
    DECLARE @column_name NVARCHAR(128);
    DECLARE @alter_sql NVARCHAR(MAX); -- <-- THIS WAS THE MISSING DECLARATION

    IF EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = PARSENAME(@target_table, 2) 
          AND TABLE_NAME = PARSENAME(@target_table, 1)
    )
    BEGIN
        -- 1. Split the DDL string into individual column definitions (rows) using the comma delimiter.
        -- 2. Extract just the column name from the definition (e.g., '[COUNTRY] VARCHAR(3)' -> '[COUNTRY]').
        -- 3. Compare this list of expected column names against the actual columns in the target table.
        IF EXISTS (
            SELECT 
                SUBSTRING(
                    RTRIM(LTRIM(REPLACE(value, '[', ''))), 
                    1, 
                    CHARINDEX(']', RTRIM(LTRIM(REPLACE(value, '[', '')))) - 1
                ) AS Column_Name
            FROM STRING_SPLIT(@expected_ddl, ',')
            
            EXCEPT
            
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = PARSENAME(@target_table, 2)
              AND TABLE_NAME = PARSENAME(@target_table, 1)
        )
        BEGIN
            SET @columns_mismatch = 1;

            -- Backup current target table
            SET @sql_log += 'Backing up table...' + CHAR(13) + CHAR(10);
            SET @sql_exec += 'SELECT * INTO {target_schema}.{backup_table} FROM ' + @target_table + ';' + CHAR(13) + CHAR(10);
            SET @sql_log += 'SELECT * INTO {target_schema}.{backup_table} FROM ' + @target_table + ';' + CHAR(13) + CHAR(10);

            -- Prepare ALTER TABLE statements
            -- Cursor to iterate over column definitions that are expected but missing from the target table.
            DECLARE cur CURSOR FOR
            SELECT 
                L.value AS Column_Definition,
                -- Use pattern matching to extract the column name from the definition string
                SUBSTRING(
                    RTRIM(LTRIM(REPLACE(L.value, '[', ''))), 
                    1, 
                    CHARINDEX(']', RTRIM(LTRIM(REPLACE(L.value, '[', '')))) - 1
                ) AS Column_Name
            FROM STRING_SPLIT(@expected_ddl, ',') AS L
            WHERE 
                -- This subquery identifies the names of the columns that are *missing*
                SUBSTRING(
                    RTRIM(LTRIM(REPLACE(L.value, '[', ''))), 
                    1, 
                    CHARINDEX(']', RTRIM(LTRIM(REPLACE(L.value, '[', '')))) - 1
                ) 
                NOT IN (
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = PARSENAME(@target_table, 2)
                      AND TABLE_NAME = PARSENAME(@target_table, 1)
                );

            OPEN cur;
            FETCH NEXT FROM cur INTO 
                @column_definition, @column_name;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                -- Build the ALTER TABLE statement using the full DDL definition
                -- We only need to check if the definition includes 'NOT NULL' and, if so,
                -- add a default value when adding the column.
                SET @column_definition = RTRIM(LTRIM(@column_definition));

                IF CHARINDEX('NOT NULL', UPPER(@column_definition)) > 0
                BEGIN
                    -- It's NOT NULL, so we must add a default value to allow the ALTER TABLE to succeed
                    SET @column_definition = @column_definition + ' DEFAULT ' + 
                                             CASE 
                                                 WHEN UPPER(@column_definition) LIKE '%INT%' THEN '0'
                                                 WHEN UPPER(@column_definition) LIKE '%CHAR%' OR UPPER(@column_definition) LIKE '%VARCHAR%' THEN ''''''
                                                 WHEN UPPER(@column_definition) LIKE '%DATE%' OR UPPER(@column_definition) LIKE '%TIME%' THEN '''1900-01-01'''
                                                 WHEN UPPER(@column_definition) LIKE '%DECIMAL%' OR UPPER(@column_definition) LIKE '%NUMERIC%' THEN '0'
                                                 ELSE 'NULL' -- Fallback to NULL if type is ambiguous/unknown
                                             END;
                END
                ELSE IF CHARINDEX('NULL', UPPER(@column_definition)) = 0 AND CHARINDEX('NOT NULL', UPPER(@column_definition)) = 0
                BEGIN
                    -- For safety, assume NULL if neither is explicitly stated, as per SQL Server default
                    SET @column_definition = @column_definition; 
                END
                
                SET @alter_sql = 'ALTER TABLE ' + @target_table + ' ADD ' + @column_definition + ';';
                
                SET @sql_exec += @alter_sql + CHAR(13) + CHAR(10);
                SET @sql_log += @alter_sql + CHAR(13) + CHAR(10);

                FETCH NEXT FROM cur INTO 
                    @column_definition, @column_name;
            END

            CLOSE cur;
            DEALLOCATE cur;

            -- Add HashDiff update statement
            SET @sql_log += 'Updating hashdiff column...' + CHAR(13) + CHAR(10);
            SET @sql_exec += 'UPDATE ' + @target_table + ' SET [{hashdiff_column}] = {hashdiff_columns_str} WHERE IsCurrent = 1;' + CHAR(13) + CHAR(10);
            SET @sql_log += 'UPDATE ' + @target_table + ' SET [{hashdiff_column}] = {hashdiff_columns_str} WHERE IsCurrent = 1;' + CHAR(13) + CHAR(10);
        END
    END
    ELSE
    BEGIN
        SET @columns_mismatch = 1;
    END

    SELECT @columns_mismatch AS columns_mismatch, @sql_log AS alter_sql, @sql_exec AS sql_to_execute;
    """


def run_scd2_sync(conn, table_list, hashdiff, hashdiff_sync_columns_str, scd_col_check_ddl, logger, dry_run):
    cursor = conn.cursor()
    # print(f"IN SCD2 sync {table_list}\n {hashdiff}\n {hashdiff_sync_columns_str}")

    for table in table_list:
        schema = table.get('schema')
        table_name = table.get('targetname')
        staging_table = table.get('name')
        staging_schema = schema

        logger.info(f"Syncing: {schema}.{table_name} <- {staging_schema}.{staging_table}")

        sql = generate_alter_and_backup_sql(
            target_schema=schema,
            target_table=table_name,
            staging_schema=staging_schema,
            staging_table=staging_table,
            hashdiff_column=hashdiff,
            hashdiff_columns_str=hashdiff_sync_columns_str,
            scd_col_check_ddl=scd_col_check_ddl
        )

        # print(f"SQL UPDATE + {sql}")

        cursor.execute(sql)
        result = cursor.fetchone()

        columns_mismatch = result.columns_mismatch
        # print(f"Columns mismatch: {columns_mismatch}++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        # logger_columns_mismatch = str(columns_mismatch)
        #alter_sql = result.alter_sql
        sql_to_execute = result.sql_to_execute

        #logger.debug(f"Columns mismatch: {logger_columns_mismatch}")
        if columns_mismatch:
            if dry_run:
                logger.debug("DRY RUN - SQL that would be executed:\n", sql_to_execute)
            else:
                try:
                    logger.info(f"Starting schema sync for {table_name}")
                    cursor.execute("BEGIN TRANSACTION;")

                    for stmt in sql_to_execute.split(';'):
                        stmt = stmt.strip()
                        if stmt:
                            logger.info(f"Executing: {stmt}")
                            cursor.execute(stmt)
                            conn.commit()

                    conn.commit()
                    logger.info("Schema sync completed successfully.")
                    logger.status_email(f"📧 Columns mismatched in {table_name}, creating a SCD backup!")
                except Exception as e:
                    logger.error("An error occurred during schema sync. Rolling back transaction.", exc_info=True)
                    conn.rollback()
                    raise  # Re-raise the exception after rollback
        else:
            logger.info("No changes needed.")

    cursor.close()
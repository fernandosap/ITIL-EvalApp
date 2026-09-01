-- Immutable attempt identity + reversible admin archive state.
-- Runtime bootstrap applies the same column additions automatically; this file
-- exists for explicit DBA/change-management execution and is schema-agnostic.
DO
BEGIN
  DECLARE schema_name NVARCHAR(256) := CURRENT_SCHEMA;
  DECLARE col_count INTEGER;

  SELECT COUNT(*) INTO col_count FROM SYS.TABLE_COLUMNS
   WHERE SCHEMA_NAME = :schema_name AND TABLE_NAME = 'ACCESS_CODES' AND COLUMN_NAME = 'ATTEMPT_QUESTION_SET_ID';
  IF :col_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE "' || :schema_name || '"."ACCESS_CODES" ADD ("ATTEMPT_QUESTION_SET_ID" BIGINT NULL)';
  END IF;

  SELECT COUNT(*) INTO col_count FROM SYS.TABLE_COLUMNS
   WHERE SCHEMA_NAME = :schema_name AND TABLE_NAME = 'ACCESS_CODES' AND COLUMN_NAME = 'ATTEMPT_QUESTION_SET_NAME';
  IF :col_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE "' || :schema_name || '"."ACCESS_CODES" ADD ("ATTEMPT_QUESTION_SET_NAME" NVARCHAR(255) NULL)';
  END IF;

  SELECT COUNT(*) INTO col_count FROM SYS.TABLE_COLUMNS
   WHERE SCHEMA_NAME = :schema_name AND TABLE_NAME = 'ACCESS_CODES' AND COLUMN_NAME = 'ATTEMPT_QUESTION_SET_VERSION';
  IF :col_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE "' || :schema_name || '"."ACCESS_CODES" ADD ("ATTEMPT_QUESTION_SET_VERSION" INTEGER NULL)';
  END IF;

  SELECT COUNT(*) INTO col_count FROM SYS.TABLE_COLUMNS
   WHERE SCHEMA_NAME = :schema_name AND TABLE_NAME = 'ACCESS_CODES' AND COLUMN_NAME = 'ARCHIVED_AT';
  IF :col_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE "' || :schema_name || '"."ACCESS_CODES" ADD ("ARCHIVED_AT" TIMESTAMP NULL)';
  END IF;
END;

-- Simple functions
DROP FUNCTION IF EXISTS mv$addIndexToMvLog$Table;
DROP FUNCTION IF EXISTS mv$addRow$ToMv$Table;
DROP FUNCTION IF EXISTS mv$addRow$ToSourceTable;
DROP FUNCTION IF EXISTS mv$clearSpentPgMviewLogs;
DROP FUNCTION IF EXISTS mv$createMvLog$Table;
DROP FUNCTION IF EXISTS mv$createMvLogTrigger;
DROP FUNCTION IF EXISTS mv$deleteMaterializedViewRows;
DROP FUNCTION IF EXISTS mv$deletePgMview;
DROP FUNCTION IF EXISTS mv$deletePgMviewOjDetails;
DROP FUNCTION IF EXISTS mv$deletePgMviewLog;
DROP FUNCTION IF EXISTS mv$dropTable;
DROP FUNCTION IF EXISTS mv$dropTrigger;
DROP FUNCTION IF EXISTS mv$grantSelectPrivileges;
DROP FUNCTION IF EXISTS mv$insertPgMviewLogs;
DROP FUNCTION IF EXISTS mv$removeRow$FromSourceTable;
DROP FUNCTION IF EXISTS mv$truncateMaterializedView;
--Complex Functions
DROP FUNCTION IF EXISTS mv$clearAllPgMvLogTableBits;
DROP FUNCTION IF EXISTS mv$clearPgMvLogTableBits;
DROP FUNCTION IF EXISTS mv$clearPgMviewLogBit;
DROP FUNCTION IF EXISTS mv$createPgMv$Table;
DROP FUNCTION IF EXISTS mv$insertMaterializedViewRows;
DROP FUNCTION IF EXISTS mv$insertPgMview;
DROP FUNCTION IF EXISTS mv$insertOuterJoinRows;
DROP FUNCTION IF EXISTS mv$insertPgMviewOuterJoinDetails;
DROP FUNCTION IF EXISTS mv$executeMVFastRefresh;
DROP FUNCTION IF EXISTS mv$refreshMaterializedViewFast;
DROP FUNCTION IF EXISTS mv$refreshMaterializedViewFull;
DROP FUNCTION IF EXISTS mv$setPgMviewLogBit;
DROP FUNCTION IF EXISTS mv$updateMaterializedViewRows;
DROP FUNCTION IF EXISTS mv$updateOuterJoinColumnsNull;
-- Application Functions
DROP FUNCTION IF EXISTS mv$createMaterializedView;
DROP FUNCTION IF EXISTS mv$createMaterializedViewlog;
DROP FUNCTION IF EXISTS mv$refreshMaterializedView;
DROP FUNCTION IF EXISTS mv$removeMaterializedView;
DROP FUNCTION IF EXISTS mv$removeMaterializedViewLog;
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Rollup]') AND type in (N'U'))
TRUNCATE TABLE [Rollup]

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Driver]') AND type in (N'U'))
TRUNCATE TABLE [Driver]

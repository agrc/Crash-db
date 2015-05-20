SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Rollup]') AND type in (N'U'))
DROP TABLE [Rollup]

CREATE TABLE [Rollup](
	[id] [int] NOT NULL,
	[crash_date] [date] NULL,
	[pedestrian] [bit] NULL,
	[bicycle] [bit] NULL,
	[motorcycle] [bit] NULL,
	[improper_restraint] [bit] NULL,
	[dui] [bit] NULL,
	[intersection] [bit] NULL,
	[animal_wild] [bit] NULL,
	[animal_domestic] [bit] NULL,
	[rollover] [bit] NULL,
	[commercial_vehicle] [bit] NULL,
	[teenager] [bit] NULL,
	[elder] [bit] NULL,
	[dark] [bit] NULL,
 CONSTRAINT [PK_Rollup] PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]


SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Driver]') AND type in (N'U'))
DROP TABLE [Driver]


CREATE TABLE [Driver](
    [id] [int] IDENTITY NOT NULL,
    [driver_id] [int] NOT NULL,
    [crash_date] [date] NULL,
    [vehicle_count] [int] NULL,
    [contributing_cause] [nchar](100) NULL,
    [alternate_cause] [nchar](100) NULL,
    [driver_condition] [nchar](100) NULL,
    [driver_distraction] [nchar](100) NULL
 CONSTRAINT [PK_Driver] PRIMARY KEY CLUSTERED
(
    [id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]


SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

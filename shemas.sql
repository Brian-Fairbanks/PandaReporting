CREATE TABLE [FireIncidents] (
  [Incident_Number] varchar(8) PRIMARY KEY,
  [Calltaker_Agency] varchar(20),
  [Address_of_Incident] varchar(200),
  [City] varchar(40),
  [Jurisdiction] varchar(40),
  [Response_Area] varchar(40),
  [Problem] varchar(80),
  [Incident_Type] varchar(40),
  [Response_Plan] varchar(40),
  [Priority_Description] varchar(8),
  [Alarm_Level] varchar(2),
  [Map_Info] varchar(40),
  [X_Long] float,
  [Y_Lat] float,
  [ESD02_Shift] varchar(8),
  [Phone_Pickup_Time] datetime,
  [Call_Entered_in_Queue] datetime,
  [First_Unit_Assigned] datetime,
  [First_Unit_Enroute] datetime,
  [First_Unit_Staged] datetime,
  [First_Unit_Arrived] datetime,
  [Call_Closed] datetime,
  [Last_Unit_Cleared] datetime,
  [Incident_Call_Disposition] varchar(40),
  [Incident_Call_Reason] varchar(40),
  [EMS_Incident_Numbers] varchar(200),
  [IsESD17] bit,
  [isETJ] bit,
  [isCOP] bit,
  [People_Per_Mile] float,
  [Population_Classification] varchar(40),
  [Closest_Station] varchar(10)
)
GO


CREATE TABLE [FireUnits] (
  [Station] varchar(40),
  [Status] varchar(8),
  [Incident_Number] varchar(8),
  [Unit] varchar(16),
  [Department] varchar(40),
  [Frontline_Status] varchar(16),
  [Location_At_Assign_Time] varchar(200),
  [First_Assign] bit,
  [FirstArrived] bit,
  [Unit_Assigned] datetime,
  [Unit_Enroute] datetime,
  [Unit_Staged] datetime,
  [Unit_Arrived] datetime,
  [Unit_Cleared] datetime,
  [Unit_Disposition] varchar(40),
  [Unit_Cancel_Reason] varchar(40),
  [Unit_Type] varchar(24),
  [Bucket_Type] varchar(24),
  [Unit_Usage_At_Time_of_Alarm] integer,
  [Assigned_at_Station] bit,
  [Distance_to_S01_in_miles] float,
  [Distance_to_S02_in_miles] float,
  [Distance_to_S03_in_miles] float,   
  [Distance_to_S04_in_miles] float,
  [Distance_to_S05_in_miles] float,
  [Distance_to_S06_in_miles] float,
  [Distance_to_S07_in_miles] float,
  [Distance_to_S08_in_miles] float,
  [Distance_to_S09_in_miles] float,
  PRIMARY KEY ([Incident_Number], [Unit], [Unit_Assigned])
)
GO

ALTER TABLE [FireUnits] ADD FOREIGN KEY ([Incident_Number]) REFERENCES [FireIncidents] ([Incident_Number])


-- Stored Procedure to update Fire and EMS link data
use [UNIT_RUN_DATA]
GO

CREATE PROCEDURE dbo.linkFireEMS @lastRunDate datetime
AS
 insert into EMS_Fire_Link
 SELECT fi.Phone_Pickup_Time, fi.Incident_Number as Fire_Incident_Number, fi.EMS_Incident_Numbers as Fire_EMS_Incident_Links, ei.Incident_Number as EMS_Incident_Number
  FROM 
  (select *
	from [UNIT_RUN_DATA].[dbo].[FireIncidents]
	where EMS_Incident_Numbers is not NULL
	and Phone_Pickup_Time > @lastRunDate
	)as fi
  inner join
  [UNIT_RUN_DATA].[dbo].[EMSIncidents] ei
  on fi.EMS_Incident_Numbers like '%'+ei.Incident_Number+'%' 
  where ei.Phone_Pickup_Time > @lastRunDate
  and not exists(select * from EMS_Fire_Link efl
	where efl.EMS_Incident_Number = ei.Incident_Number
	and efl.Fire_Incident_Number = fi.Incident_Number
	)
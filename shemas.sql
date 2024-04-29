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

[initialDispatchCode],
[workingFire],
[CriticalIncident],
[CriticalIncidentTeamMobilized],
[reportToIRWIN],

  use [ESO]
  CREATE TABLE Basic (
    [IncidentId] NVARCHAR(36) NOT NULL PRIMARY KEY,
    [IncidentNumber] NVARCHAR(50) NULL,
    [NFIRSIncidentNumber] NVARCHAR(7) NOT NULL,
    [IncidentDate] DATETIME2 NULL,
    [IncidentType] NVARCHAR(50) NULL,
    [Alarms] INT NULL,
    [Station] NVARCHAR(50) NULL,
    [StationName] NVARCHAR(100) NULL,
    [Shift] NVARCHAR(10) NULL,
    [District] NVARCHAR(50) NULL,
    [DistrictName] NVARCHAR(100) NULL,
    [ResourceFormUsed] BIT NULL,
    [fireServiceDeaths] BIGINT NULL,
    [otherDeaths] BIGINT NULL,
    [fireServiceInjuries] BIGINT NULL,
    [otherInjuries] BIGINT NULL,
    [HazardousMaterialReleased] NVARCHAR(50) NULL,
    [fireDiscoveryDatetime] DATETIME2 NULL,
    [PSAPDatetime] DATETIME2 NULL,
    [AlarmDatetime] DATETIME2 NULL,
    [DispatchNotifiedDatetime] DATETIME2 NULL,
    [ArrivalDatetime] DATETIME2 NULL,
    [WaterOnFireDatetime] DATETIME2 NULL,
    [LossStopDatetime] DATETIME2 NULL,
    [AtPatientDatetime] DATETIME2 NULL,
    [IncidentControlledDateTime] DATETIME2 NULL,
    [LastUnitClearedDatetime] DATETIME2 NULL,
    [ActionsTaken] NVARCHAR(MAX) NULL,
    [CriticalIncidentCircumstances] NVARCHAR(MAX) NULL,
    [AidGivenOrReceived] NVARCHAR(50) NULL,
    [AidedAgency] NVARCHAR(100) NULL,
    [AidedAgencyIncidentNumber] NVARCHAR(50) NULL,
    [AidingAgencies] NVARCHAR(100) NULL,
    [AddressOnWildland] BIT NULL,
    [LocationTypeCode] NVARCHAR(50) NULL,
    [LocationType] NVARCHAR(50) NULL,
    [PropertyUseCode] NVARCHAR(50) NULL,
    [PropertyUse] NVARCHAR(50) NULL,
    [MixedUse] NVARCHAR(50) NULL,
    [CensusTract] NVARCHAR(50) NULL,
    [DetectorAlertedOccupants] NVARCHAR(50) NULL,
    [NumberOrMilepost] NVARCHAR(50) NULL,
    [StreetPrefixCode] NVARCHAR(10) NULL,
    [StreetPrefix] NVARCHAR(50) NULL,
    [StreetOrHighwayName] NVARCHAR(100) NULL,
    [StreetTypeCode] NVARCHAR(10) NULL,
    [StreetType] NVARCHAR(50) NULL,
    [StreetSuffix] NVARCHAR(50) NULL,
    [PostOfficeBox] NVARCHAR(50) NULL,
    [ApartmentNumber] NVARCHAR(50) NULL,
    [City] NVARCHAR(50) NULL,
    [State] NVARCHAR(50) NULL,
    [Zip] NVARCHAR(10) NULL,
    [County] NVARCHAR(100) NULL,
    [Directions] NVARCHAR(MAX) NULL,
    [Latitude] FLOAT NULL,
    [Longitude] FLOAT NULL,
    [personsAndEntities] NVARCHAR(MAX) NULL,
    [OwnerSequenceNumber] BIGINT NULL,
    [OwnerNameDetails] NVARCHAR(MAX) NULL,
    [OwnerAddressSameAsIncident] BIT NULL,
    [OwnerAddressDetails] NVARCHAR(MAX) NULL,
    [OwnerBusinessName] NVARCHAR(100) NULL,
    [OwnerPhone] NVARCHAR(50) NULL,
    [OwnerAffiliation] NVARCHAR(50) NULL,
    [OwnerInsuranceCompany] NVARCHAR(100) NULL,
    [OwnerTotalInsuranceAmount] FLOAT NULL,
    [OwnerInvolvedInIncident] BIT NULL,
    [vehicles] NVARCHAR(MAX) NULL,
    [PropertyLoss] FLOAT NULL,
    [PropertyValue] FLOAT NULL,
    [ContentsLoss] FLOAT NULL,
    [ContentsValue] FLOAT NULL,
    [PropertyLossIsNone] BIT NULL,
    [PropertyValueIsNone] BIT NULL,
    [ContentsLossIsNone] BIT NULL,
    [ContentsValueIsNone] BIT NULL,
    [apparatusesAndPersonnel] NVARCHAR(MAX) NULL,
    [SuppressionApparatusCount] BIGINT NULL,
    [SuppressionPersonnelCount] BIGINT NULL,
    [EmsApparatusCount] BIGINT NULL,
    [EmsPersonnelCount] BIGINT NULL,
    [OtherApparatusCount] BIGINT NULL,
    [OtherPersonnelCount] BIGINT NULL,
    [ResourcesIncludeMutualAid] BIT NULL,
    [ReportWriterName] NVARCHAR(100) NULL,
    [ReportWriterAssignment] NVARCHAR(100) NULL,
    [ReportWriterRank] NVARCHAR(50) NULL,
    [OfficerInCharge] NVARCHAR(100) NULL,
    [OfficerInChargeAssignment] NVARCHAR(100) NULL,
    [OfficerInChargeRank] NVARCHAR(50) NULL,
    [Narratives] NVARCHAR(MAX) NULL,
    [fireDetails] NVARCHAR(MAX) NULL,
    [civilianCasualties] NVARCHAR(MAX) NULL,
    [emsPatients] NVARCHAR(MAX) NULL,
    [hazmatDetails] NVARCHAR(MAX) NULL,
    [wildlandDetails] NVARCHAR(MAX) NULL,
    [arsonDetails] NVARCHAR(MAX) NULL,
    [cadNotes] NVARCHAR(MAX) NULL,
    [Covid19Factor] NVARCHAR(50) NULL,
    [temporaryResidentInvolvement] NVARCHAR(50) NULL,
    [qualityControlFirstName] NVARCHAR(50) NULL,
    [qualityControlLastName] NVARCHAR(50) NULL,
    [qualityControlCompletedDate] DATETIME2 NULL
);

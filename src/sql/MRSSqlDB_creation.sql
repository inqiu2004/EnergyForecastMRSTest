USE [msdb]
GO

/* enable agent */
sp_configure 'show advanced options',1
go
reconfigure with override
go
sp_configure 'Agent XPs',1
go
reconfigure with override
go
sp_configure 'show advanced options',0
go
reconfigure with override
go

USE $(DBName)
GO

/* create database objects */
IF OBJECT_ID('dbo.usp_delete_job', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_delete_job]
GO

IF OBJECT_ID('dbo.usp_create_job', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_create_job]
GO

IF OBJECT_ID('dbo.usp_GenerateHistorcialData', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_GenerateHistorcialData]
GO

IF OBJECT_ID('dbo.usp_Data_Simulator_Demand', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_Data_Simulator_Demand]
GO

IF OBJECT_ID('dbo.usp_Data_Simulator_Temperature', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_Data_Simulator_Temperature]
GO

IF OBJECT_ID('dbo.usp_featureEngineering', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_featureEngineering]
GO

IF OBJECT_ID('dbo.usp_persistModel', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_persistModel]
GO

IF OBJECT_ID('dbo.usp_trainModel', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_trainModel]
GO

IF OBJECT_ID('dbo.usp_predictDemand', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_predictDemand]
GO

IF OBJECT_ID('dbo.usp_energyDemandForecastMain', 'P') IS NOT NULL
  DROP PROCEDURE [dbo].[usp_energyDemandForecastMain]
GO

IF OBJECT_ID('dbo.DemandSeed', 'U') IS NOT NULL
  DROP TABLE [dbo].[DemandSeed]
GO

CREATE TABLE [dbo].[DemandSeed] (
	[utcTimestamp]	DATETIME		NOT NULL,
    [region]      	NVARCHAR(64)     NOT NULL,
    [Load]  		FLOAT (53) 		NULL
);
go

IF OBJECT_ID('dbo.TemperatureSeed', 'U') IS NOT NULL
  DROP TABLE [dbo].[TemperatureSeed]
GO

CREATE TABLE [dbo].[TemperatureSeed] (
	[utcTimestamp]	DATETIME		NOT NULL,
    [region]      	NVARCHAR(64)     NOT NULL,
    [Temperature]  	FLOAT (53) 		NULL,
	[Flag]			INT				NOT NULL
);
go

IF OBJECT_ID('dbo.DemandReal', 'U') IS NOT NULL
  DROP TABLE [dbo].[DemandReal]
GO

CREATE TABLE [dbo].[DemandReal] (
    [utcTimestamp] 	DATETIME   		NOT NULL,
    [region]      	NVARCHAR(64)     NOT NULL,
    [Load]  		FLOAT (53) 		NULL,
    CONSTRAINT [PK_DemandReal] PRIMARY KEY CLUSTERED ( [utcTimestamp] ASC, [region] ASC)
);
go

IF OBJECT_ID('dbo.TemperatureReal', 'U') IS NOT NULL
  DROP TABLE [dbo].[TemperatureReal]
GO

CREATE TABLE [dbo].[TemperatureReal] (
    [utcTimestamp] 	DATETIME   NOT NULL,
    [region]      	NVARCHAR(64)     NOT NULL,
    [Temperature]  	FLOAT (53) 		NULL,
	[Flag]			INT				NOT NULL,
    CONSTRAINT [PK_TemperatureReal] PRIMARY KEY CLUSTERED ( [utcTimestamp] ASC, [region] ASC)
);
go

IF OBJECT_ID('dbo.DemandForecast', 'U') IS NOT NULL
  DROP TABLE [dbo].[DemandForecast]
GO

IF OBJECT_ID('dbo.Model', 'U') IS NOT NULL
  DROP TABLE [dbo].[Model]
GO
CREATE TABLE [dbo].Model (
	[Model] 		varbinary(max)	NOT NULL,
    [region]      	NVARCHAR(64)     NOT NULL,
	[startTime]		NVARCHAR(50)		NOT NULL,
    CONSTRAINT [PK_Model] PRIMARY KEY CLUSTERED ( [region] ASC, startTime ASC)
);

IF OBJECT_ID('dbo.DemandForecast', 'U') IS NOT NULL
  DROP TABLE [dbo].[DemandForecast]
GO

CREATE TABLE [dbo].[DemandForecast] (
    [utcTimestamp] 	DATETIME   		NOT NULL,
    [region]      	NVARCHAR(64)     NOT NULL,
    [Load]  		FLOAT (53) 		NULL,
    CONSTRAINT [PK_DemandForecast] PRIMARY KEY CLUSTERED ( [utcTimestamp] ASC, [region] ASC)
);
go

IF OBJECT_ID('dbo.InputAllFeatures', 'U') IS NOT NULL
  DROP TABLE [dbo].[InputAllFeatures]
GO

CREATE TABLE InputAllFeatures(
   utcTimestamp datetime,
   region varchar(64),
	Load float,   
   temperature float,
   lag24 float,
   lag25 float,
   lag26 float,
   lag27 float,
   lag28 float,
   lag31 float,
   lag36 float,
   lag40 float,
   lag48 float,
   lag72 float,
   lag96 float,
   hourofday tinyint,
   dayinweek tinyint,
   monofyear tinyint,
   weekend tinyint,
   businesstime tinyint,
   ismorning tinyint,
   LinearTrend float,
   WKFreqCos1 float,
   WKFreqSin1 float,
   WDFreqCos1 float,
   WDFreqSin1 float,
   WKFreqCos2 float,
   WKFreqSin2 float,
   WDFreqCos2 float,
   WDFreqSin2 float,
    CONSTRAINT [PK_InputAllFeatures] PRIMARY KEY CLUSTERED ( [utcTimestamp] ASC, [region] ASC)	   );
GO	   

CREATE PROCEDURE [dbo].[usp_GenerateHistorcialData] 
AS
BEGIN
	SET NOCOUNT ON;
	
	declare @maxDemandTimestamp datetime;
	declare @maxTemperatureTimestamp datetime;	
	declare @currTimestamp datetime;
	
	select @currTimestamp=GETUTCDATE();
	select @maxDemandTimestamp = max(utcTimestamp) from DemandSeed;
	select @maxTemperatureTimestamp = max(utcTimestamp) from TemperatureSeed;	
	
	MERGE DemandReal as target
	USING (
			select	dateadd(mi, datediff(mi, @maxDemandTimestamp,dateadd(minute, datediff(minute,0,@currTimestamp) / 15 * 15, 0)), utcTimestamp) as utcTimestamp,
				region, Load
			from DemandSeed
	)	as source
	ON (target.region = source.region and target.utcTimestamp=source.utcTimestamp)	
	WHEN MATCHED THEN 
		UPDATE SET load= source.load
	WHEN NOT MATCHED THEN
		INSERT (utcTimestamp, region, load)
		VALUES (source.utcTimestamp, source.region, source.load); 

	MERGE TemperatureReal as target
	USING (
		select	dateadd(mi, datediff(mi, @maxTemperatureTimestamp,dateadd(minute, datediff(minute,0,dateadd(hour,6,@currTimestamp)) / 60 * 60, 0)), utcTimestamp) as utcTimestamp,
		region, Temperature, Flag
		from TemperatureSeed
	) as source
	ON (target.region = source.region and target.utcTimestamp=source.utcTimestamp)	
	WHEN MATCHED THEN 
		UPDATE SET temperature= source.temperature, flag= source.flag
	WHEN NOT MATCHED THEN
		INSERT (utcTimestamp, region, temperature, flag)
		VALUES (source.utcTimestamp, source.region, source.temperature, source.flag);

END;
Go

CREATE PROCEDURE usp_Data_Simulator_Demand
AS
SET NOCOUNT ON;
BEGIN   
	declare @currTimestamp1 datetime;
	declare @currTimestamp2 datetime;	
	
	select @currTimestamp1 = dateadd(minute, datediff(minute,0,GETUTCDATE()) / 15 * 15, 0)
	
	IF convert(NVARCHAR(5), @currTimestamp1, 110) = '02-29'
	BEGIN
		MERGE DemandReal as target
		USING (
			SELECT concat(datepart(year,@currTimestamp1),'-',convert(NVARCHAR(5), utctimestamp, 110), ' ', convert(NVARCHAR(8), cast(utctimestamp as time),108)) as utcTimeStamp,
					region, round(load*(RAND(CHECKSUM(NEWID()))*(105.99-94.99)+94.99)/100,1) as load
			from DemandSeed 
			where concat(convert(NVARCHAR(5), utctimestamp, 110), ' ', cast(utctimestamp as time)) 
					=concat(convert(NVARCHAR(5), @currTimestamp1, 110), ' ', cast(@currTimestamp1 as time))
		) as source
		ON (target.region = source.region and target.utcTimestamp=source.utcTimestamp)	
		WHEN MATCHED THEN 
			UPDATE SET load= source.load
		WHEN NOT MATCHED THEN
			INSERT (utcTimestamp, region, load)
			VALUES (source.utcTimestamp, source.region, source.load); 		
	END
	ELSE
	BEGIN
		MERGE DemandReal as target
		USING (	
			SELECT concat(datepart(year,@currTimestamp1),'-',convert(NVARCHAR(5), @currTimestamp1, 110), ' ', convert(NVARCHAR(8), cast(utctimestamp as time),108)) as utcTimeStamp,
					region, round(load*(RAND(CHECKSUM(NEWID()))*(105.99-94.99)+94.99)/100,1) as load
			from DemandSeed 
			where concat(convert(NVARCHAR(5), utcTimeStamp, 110), ' ', cast(utctimestamp as time)) 
				=concat(convert(NVARCHAR(5), dateadd(day, 1, @currTimestamp1), 110), ' ', cast(@currTimestamp1 as time))
		) as source
		ON (target.region = source.region and target.utcTimestamp=source.utcTimestamp)	
		WHEN MATCHED THEN 
			UPDATE SET load= source.load
		WHEN NOT MATCHED THEN
			INSERT (utcTimestamp, region, load)
			VALUES (source.utcTimestamp, source.region, source.load); 	
	END
END;
GO


CREATE PROCEDURE usp_Data_Simulator_Temperature
AS
SET NOCOUNT ON;
BEGIN   
	declare @currTimestamp2 datetime;	
	
	--select @currTimestamp2 = dateadd(minute, datediff(minute,0,GETUTCDATE()) / 60 * 60, 0)
	select @currTimestamp2 = dateadd(minute, datediff(minute,0,dateadd(hour,6,GETUTCDATE())) / 60 * 60, 0)
	
	IF convert(NVARCHAR(5), @currTimestamp2, 110) = '02-29'
	BEGIN
		MERGE TemperatureReal as target
		USING (
			SELECT concat(datepart(year,@currTimestamp2),'-',convert(NVARCHAR(5), utctimestamp, 110), ' ', convert(NVARCHAR(8), cast(utctimestamp as time),108)) as utcTimeStamp,
					region, round(temperature*(RAND(CHECKSUM(NEWID()))*(105.99-94.99)+94.99)/100,1) as temperature, 
					flag 
			from TemperatureSeed 
			where concat(convert(NVARCHAR(5), utctimestamp, 110), ' ', cast(utctimestamp as time)) 
					=concat(convert(NVARCHAR(5), @currTimestamp2, 110), ' ', cast(@currTimestamp2 as time))
		) as source
		ON (target.region = source.region and target.utcTimestamp=source.utcTimestamp)	
		WHEN MATCHED THEN 
			UPDATE SET temperature= source.temperature, flag= source.flag
		WHEN NOT MATCHED THEN
			INSERT (utcTimestamp, region, temperature, flag)
			VALUES (source.utcTimestamp, source.region, source.temperature, source.flag); 
	END
	ELSE
	BEGIN
		MERGE TemperatureReal as target
		USING (	
			SELECT concat(datepart(year,@currTimestamp2),'-',convert(NVARCHAR(5), @currTimestamp2, 110), ' ', convert(NVARCHAR(8), cast(utctimestamp as time),108)) as utcTimeStamp,
					region, round(temperature*(RAND(CHECKSUM(NEWID()))*(105.99-94.99)+94.99)/100,1) as temperature, flag 
			from TemperatureSeed 
			where concat(convert(NVARCHAR(5), utcTimeStamp, 110), ' ', cast(utctimestamp as time)) 
				=concat(convert(NVARCHAR(5), dateadd(day, 1, @currTimestamp2), 110), ' ', cast(@currTimestamp2 as time))
		) as source
		ON (target.region = source.region and target.utcTimestamp=source.utcTimestamp)	
		WHEN MATCHED THEN 
			UPDATE SET temperature= source.temperature, flag= source.flag
		WHEN NOT MATCHED THEN
			INSERT (utcTimestamp, region, temperature, flag)
			VALUES (source.utcTimestamp, source.region, source.temperature, source.flag); 	

	END
END;
GO

CREATE PROCEDURE [dbo].[usp_featureEngineering] (
	@region NVARCHAR(64),
	@startTime VARCHAR(50), 
	@endTime VARCHAR(50),
	@scoreStartTime VARCHAR(50),
	@scoreEndTime VARCHAR(50),
	@server VARCHAR(255),
	@database VARCHAR(255),
	@user VARCHAR(255),
	@pwd VARCHAR(255))
AS
BEGIN
	DECLARE @InputAllFeaturesTable NVARCHAR(50);
	DECLARE @numTS bigint;
	SET @numTS= cast(datediff(minute,@startTime,@scoreEndTime) as bigint);

	DECLARE @InputData TABLE (
		utcTimestamp 	DATETIME,
		Load 			float,
		temperature 	float
	);

	DECLARE @InputDataNAfilled TABLE (
		utcTimestamp 	DATETIME,
		Load 			float,
		temperature 	float
	);

	delete InputAllFeatures where region=@region;

	with TimeSequence as
	(
		Select cast(@scoreStartTime as datetime) as utcTimestamp
			union all
		Select dateadd(minute, 15, utcTimestamp)
			from TimeSequence
			where utcTimestamp < cast(@scoreEndTime as datetime)
	),
	e1(n) AS
	(
		SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
		SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
		SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
	), 
	e2(n) AS (SELECT 1 FROM e1 CROSS JOIN e1 AS b), 
	e3(n) AS (SELECT 1 FROM e2 CROSS JOIN e2 AS b), 
	e4(n) AS (SELECT 1 FROM e3 CROSS JOIN (SELECT TOP 5 n FROM e1) AS b)
	insert into @InputData 
	select e.utcTimestamp as utcTimestamp, Load, temperature 
	from 
	(
		SELECT CONVERT(varchar(50),DATEADD(minute, n, CONVERT(datetime,@startTime,120)),120) utcTimestamp
			FROM
			(
			  SELECT ((ROW_NUMBER() OVER (ORDER BY n))-1)*15 n FROM e4
			 ) as d
		   where n<=@numTS
	) as e
	left join
	(
		select a.utcTimestamp as utcTimestamp, b.region, a.Load, b.temperature from (
			select utcTimestamp,Load from dbo.DemandReal where region=@region and utcTimestamp>=@startTime and utcTimestamp<=@endTime
				union all 
				select convert(NVARCHAR(50),utcTimestamp,120) as utcTimestamp, NULL as Load from TimeSequence
				) as a
		right join 
			(select utcTimestamp, region, temperature from dbo.TemperatureReal where region=@region and utcTimestamp>=@startTime and utcTimestamp<=@scoreEndTime
				) as b 
		on dateadd(hour, datediff(hour, 0, CAST(a.utcTimestamp as datetime)), 0)
			= dateadd(hour, datediff(hour, 0, CAST(b.utcTimestamp as datetime)), 0) 
	) as c
	on e.utcTimestamp=c.utcTimestamp 
	order by e.utcTimestamp;

	DECLARE @avgLoad float;
	DECLARE @avgTemp float;
	SELECT @avgLoad=avg(Load), @avgTemp = avg(temperature) from @InputData;

	Insert into @InputDataNAfilled
	SELECT utcTimestamp,
		(CASE WHEN Load is NULL and loadLag96 is NULL THEN @avgLoad
			  WHEN Load is NULL and loadLag96 is not NULL THEN loadLag96
			  ELSE Load END) as Load,
		(CASE WHEN temperature is NULL and tempLag96 is NULL THEN @avgTemp
			  WHEN temperature is NULL and tempLag96 is not NULL THEN tempLag96
			  ELSE temperature END) as temperature
	from 
	(SELECT utcTimestamp,Load,temperature,
		LAG(Load,96,NULL) OVER (ORDER BY utcTimestamp) as loadLag96,
		LAG(temperature,96,NULL) OVER (ORDER BY utcTimestamp) as tempLag96 
	from @InputData) as a
	order by utcTimestamp

	Insert INTO InputAllFeatures
	SELECT 
		utcTimestamp,@region, Load,temperature,
		LAG(Load,24,NULL) OVER (ORDER BY utcTimestamp) as lag24,
		LAG(Load,25,NULL) OVER (ORDER BY utcTimestamp) as lag25,
		LAG(Load,26,NULL) OVER (ORDER BY utcTimestamp) as lag26,
		LAG(Load,27,NULL) OVER (ORDER BY utcTimestamp) as lag27,
		LAG(Load,28,NULL) OVER (ORDER BY utcTimestamp) as lag28,
		LAG(Load,31,NULL) OVER (ORDER BY utcTimestamp) as lag31,
		LAG(Load,36,NULL) OVER (ORDER BY utcTimestamp) as lag36,
		LAG(Load,40,NULL) OVER (ORDER BY utcTimestamp) as lag40,
		LAG(Load,48,NULL) OVER (ORDER BY utcTimestamp) as lag48,
		LAG(Load,72,NULL) OVER (ORDER BY utcTimestamp) as lag72,
		LAG(Load,96,NULL) OVER (ORDER BY utcTimestamp) as lag96,
		hourofday, dayinweek, monofyear, weekend,
		(case when hourofday<=18 and hourofday>=8 then 1 else 0 end) as businesstime,
		(case when hourofday>=5 and hourofday<=8 then 1 else 0 end) as ismorning,
		t/365.25 as LinearTrend,
		cos(t*2*pi()/365.25)*weekend as WKFreqCos1,
		sin(t*2*pi()/365.25)*weekend as WKFreqSin1,
		cos(t*2*pi()/365.25)*(1-weekend) as WDFreqCos1,
		sin(t*2*pi()/365.25)*(1-weekend) as WDFreqSin1,
		cos(t*2*pi()*2/365.25)*weekend as WKFreqCos2,
		sin(t*2*pi()*2/365.25)*weekend as WKFreqSin2,
		cos(t*2*pi()*2/365.25)*(1-weekend) as WDFreqCos2,
		sin(t*2*pi()*2/365.25)*(1-weekend) as WDFreqSin2
	 from (
		select 	utcTimestamp,Load,temperature,
				datepart(hour, utcTimestamp) as hourofday, 
				datepart(weekday, utcTimestamp) as dayinweek, 
				datepart(month, utcTimestamp) as monofyear,
				(case when datepart(weekday, utcTimestamp) in (1,7) then 1 else 0 end) as weekend,
				floor((convert(float, ROW_NUMBER() OVER (ORDER BY utcTimestamp))-1)/24) as t			
		from (
			select convert(datetime,utcTimestamp,120) as utcTimestamp,Load,temperature 
			from @InputDataNAfilled
			) as a
	) as b;
END;
GO

-- stored procedure for model training
CREATE PROCEDURE [dbo].[usp_trainModel] 
	@queryStr nvarchar(max),
	@server varchar(255),
	@database varchar(255),
	@user varchar(255),
	@pwd varchar(255)
AS
BEGIN
	EXEC sp_execute_external_script @language = N'R',
								  @script = N'
									sqlConnString <- paste("Driver=SQL Server;Server=",serverName,";Database=",dbName,";Uid=",user,";Pwd=",password,sep="")
									sqlShareDir <- paste("c:\\AllShare\\", Sys.getenv("USERNAME"), sep="")
									dir.create(sqlShareDir, recursive = TRUE,showWarnings = FALSE)
									sqlWait <- TRUE
									sqlConsoleOutput <- TRUE
									sqlRowsPerRead <- 5000
									sqlCompute <- RxInSqlServer(
									connectionString = sqlConnString,
									shareDir = sqlShareDir,
									wait = sqlWait,
									consoleOutput = sqlConsoleOutput,
									traceEnabled = TRUE,
									traceLevel = 7)

									rxSetComputeContext(sqlCompute)

									edfFeaturesTrainSQL =  RxSqlServerData(sqlQuery = query,connectionString = sqlConnString,rowsPerRead = sqlRowsPerRead)
									labelVar = "Load"
									featureVars = rxGetVarNames(edfFeaturesTrainSQL)
									featureVars = featureVars[which((featureVars!=labelVar)&(featureVars!="region")&(featureVars!="utcTimestamp"))]
									formula = as.formula(paste(paste(labelVar,"~"),paste(featureVars,collapse="+")))

									regForest = rxDForest(formula, data = edfFeaturesTrainSQL)

									modelbin <- as.raw(serialize(regForest, NULL))

									OutputDataSet = data.frame(model=modelbin)',
								  @input_data_1 = N'select getdate()',  --The input dataset is not actually used, but this parameter is required by the stored procedure
								  @params = N'@query varchar(max), @serverName varchar(255), @dbName varchar(255), @user varchar(255), @password varchar(255)',
								  @query = @queryStr,
								  @serverName = @server,
								  @dbName = @database,
								  @user = @user,
								  @password=@pwd
								  WITH RESULT SETS ((model varbinary(max)));
END;
GO

-- stored procedure for persisting model
CREATE PROCEDURE [dbo].[usp_persistModel] 
	@region VARCHAR(10),
	@scoreStartTime VARCHAR(50),
	@server VARCHAR(255),
	@database VARCHAR(255),
	@user VARCHAR(255),
	@pwd VARCHAR(255)
AS
BEGIN
	DECLARE @ModelTable TABLE 
	(model varbinary(max))

	DECLARE @queryStr VARCHAR(max)
	set @queryStr = concat('select * from inputAllfeatures where region=''',  @region, ''' and utcTimestamp < ''',  @scoreStartTime , '''')

	INSERT INTO @ModelTable EXEC usp_trainModel @queryStr = @queryStr,@server=@server, @database=@database, @user=@user, @pwd=@pwd

	Merge Model as target
		USING (select @region as region,model from @ModelTable) as source
	on target.region = source.region
	WHEN MATCHED THEN 
		UPDATE SET target.model= source.model
	WHEN NOT MATCHED THEN
		INSERT (model, region, startTime) values (source.model,source.region,@scoreStartTime);
END;
GO

CREATE PROCEDURE [dbo].[usp_predictDemand] 
	@queryStr NVARCHAR(max),
	@region VARCHAR(64),
	@startTime VARCHAR(50)
AS
BEGIN
	DECLARE @regForestModel varbinary(max) = (SELECT TOP 1 model FROM Model where region=@region and startTime=@startTime);
	EXEC sp_execute_external_script @language = N'R',
                                  @script = N'
									mod <- unserialize(as.raw(model));
									print(summary(mod))
									OutputDataSet<-rxPredict(modelObject = mod, data = InputDataSet, outData = NULL, 
									type = "response", extraVarsToWrite=c("utcTimestamp"), overwrite = TRUE);
									str(OutputDataSet)
									print(OutputDataSet)',
                                  @input_data_1 = @queryStr,
								  @params = N'@model varbinary(max)',
                                  @model = @regForestModel
	WITH RESULT SETS ((Load_Pred float, utcTimestamp NVARCHAR(50)));
END;
GO

CREATE PROCEDURE [dbo].[usp_energyDemandForecastMain]
	@region nvarchar(10),
	@server varchar(255),
	@database varchar(255),
	@user varchar(255),
	@pwd varchar(255)
AS
BEGIN
	--declare parameters
	DECLARE @curTime datetime;	
	DECLARE @startTime varchar(50);
	DECLARE @endTime varchar(50);
	DECLARE @scoreStartTime varchar(50);
	DECLARE @scoreEndTime varchar(50);

	--set values
	SET @curTime = dateadd(minute, datediff(minute,0,GETUTCDATE()) / 15 * 15, 0)	
	SET @startTime=CONVERT(varchar(50),DATEADD(year,-1,@curTime),20);
	SET @endTime=CONVERT(varchar(50),@curTime,20);
	SET @scoreStartTime = CONVERT(varchar(50),DATEADD(minute,15,@curTime),20);
	SET @scoreEndTime=CONVERT(varchar(50),DATEADD(hour,6,@curTime),20);

	--feaure engineering
	EXEC usp_featureEngineering @region, @startTime, @endTime, @scoreStartTime, @scoreEndTime, @server, @database, @user, @pwd;

	--train model and persist
	EXEC usp_persistModel @region, @scoreStartTime, @server, @database, @user, @pwd; 

	--forecast
	Declare @tmpTable TABLE (
	load float,
	utcTimestamp varchar(50))

	DECLARE @predictQuery NVARCHAR(MAX) = concat('select * from inputAllfeatures where region=''',  @region, ''' and utcTimestamp >= ''',  @scoreStartTime , '''')

	INSERT INTO @tmpTable EXEC usp_predictDemand @querystr = @predictQuery, @region=@region, @startTime=@scoreStartTime;
					
	MERGE DemandForecast as target 
		USING @tmpTable as source
		on (target.utcTimestamp=source.utcTimestamp and target.region=@region)
		WHEN MATCHED THEN
			UPDATE SET Load = source.Load
		WHEN NOT MATCHED THEN
			INSERT (utcTimestamp, region, load)
			VALUES (source.utcTimestamp, @region, source.load);
END;
GO

create procedure usp_delete_job (@dbname NVARCHAR(64))
as
BEGIN
	DECLARE @jobId binary(16)
	DECLARE @jobName NVARCHAR(64)
	
	-- delete demand data simulator
	set @jobName = concat(@dbname,'_',N'Energy_Demand_data_simulator')
		
	SELECT @jobId = job_id FROM msdb.dbo.sysjobs WHERE (name = @jobName)
	IF (@jobId IS NOT NULL)
	BEGIN
		EXEC msdb.dbo.sp_delete_job @jobId
	END;

	-- delete temperature data simulator
	set @jobid=NULL
	set @jobName = concat(@dbname,'_',N'Energy_Temperature_data_simulator')	
	SELECT @jobId = job_id FROM msdb.dbo.sysjobs WHERE (name = @jobName)
	IF (@jobId IS NOT NULL)
	BEGIN
		EXEC msdb.dbo.sp_delete_job @jobId
	END;
	
	--delete jobs for each region
	DECLARE @MyCursor CURSOR;
	DECLARE @region varchar(64);
	DECLARE @sp NVARCHAR(200)	
	BEGIN
		SET @MyCursor = CURSOR FOR
		select distinct region from demandseed;

		OPEN @MyCursor 
		FETCH NEXT FROM @MyCursor INTO @region

		WHILE @@FETCH_STATUS = 0
		BEGIN
			set @jobid=NULL
			set @jobName = concat(upper(@dbname),'_',N'prediction_job','_',@region)
			SELECT @jobId = job_id FROM msdb.dbo.sysjobs WHERE (name = @jobName)
			IF (@jobId IS NOT NULL)
			BEGIN
				EXEC msdb.dbo.sp_delete_job @jobId
			END;	
			
			FETCH NEXT FROM @MyCursor INTO @region 
		END; 

		CLOSE @MyCursor ;
		DEALLOCATE @MyCursor;
	END;	
END;
GO

create procedure usp_create_job (@servername varchar(100), @dbname VARCHAR(64), @username varchar(64), @pswd varchar(64))
as
Begin
	BEGIN TRANSACTION
	DECLARE @ReturnCode INT
	SELECT @ReturnCode = 0
	DECLARE @jobName NVARCHAR(64)
	DECLARE @jobId BINARY(16)
	Set @jobid = NULL
	set @jobName = concat(upper(@dbname),'_',N'Energy_Demand_data_simulator')
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@jobName, 
			@description=N'Simulator for generating energy demand data in every 15 minutes', 
			@job_id = @jobId OUTPUT
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'generatorData', 
			@step_id=1, 
			@subsystem=N'TSQL', 
			@command=N'exec usp_Data_Simulator_Demand;', 
			@database_name=@dbName 
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'RunEvery15Minutes', 
			@enabled=1, 
			@freq_type=4, 
			@freq_interval=1, 
			@freq_subday_type=4, 
			@freq_subday_interval=15, 
			@freq_relative_interval=0, 
			@freq_recurrence_factor=0, 
			@active_start_date=20160222, 
			@active_end_date=99991231, 
			@active_start_time=0, 
			@active_end_time=235959
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	set @jobid=NULL
	set @jobName = concat(upper(@dbname),'_',N'Energy_Temperature_data_simulator')
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@jobName, 
			@description=N'Simulator for generating energy demand data in every 15 minutes', 
			@job_id = @jobId OUTPUT
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'generatorData', 
			@step_id=1, 
			@subsystem=N'TSQL', 
			@command=N'exec usp_Data_Simulator_Temperature;', 
			@database_name=@dbname 
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'RunEvery1Hour', 
			@enabled=1, 
			@freq_type=4, 
			@freq_interval=1, 
			@freq_subday_type=8, 
			@freq_subday_interval=1, 
			@freq_relative_interval=0, 
			@freq_recurrence_factor=0, 
			@active_start_date=20160222, 
			@active_end_date=99991231, 
			@active_start_time=0, 
			@active_end_time=235959
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	
	--create jobs for each region
	DECLARE @MyCursor CURSOR;
	DECLARE @region varchar(64);
	DECLARE @sp NVARCHAR(200)	
	BEGIN
		SET @MyCursor = CURSOR FOR
		select distinct region from demandseed;

		OPEN @MyCursor 
		FETCH NEXT FROM @MyCursor INTO @region

		WHILE @@FETCH_STATUS = 0
		BEGIN
			Set @jobid = NULL
			set @jobName = concat(upper(@dbname),'_',N'prediction_job','_',@region)
			SET @sp = N'exec [dbo].[usp_energyDemandForecastMain] ''' + @region  + ''', ''' + @servername  + ''', ''' + @dbname  + ''', ''' + @username  + ''', ''' +@pswd  + '''';
			print @sp
			EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@jobName, 
					@description=N'Predict energy demand data in every 15 minutes', 
					@job_id = @jobId OUTPUT
			IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

			EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'prediction', 
					@step_id=1, 
					@subsystem=N'TSQL', 
					@command=@sp, 
					@database_name=@dbName 
			IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
			EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
			IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
			EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'RunEvery15Minutes', 
					@enabled=1, 
					@freq_type=4, 
					@freq_interval=1, 
					@freq_subday_type=4, 
					@freq_subday_interval=15, 
					@freq_relative_interval=0, 
					@freq_recurrence_factor=0, 
					@active_start_date=20160222, 
					@active_end_date=99991231, 
					@active_start_time=0, 
					@active_end_time=235959
			IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
			EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
			IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback		
			
			FETCH NEXT FROM @MyCursor INTO @region 
		END; 

		CLOSE @MyCursor ;
		DEALLOCATE @MyCursor;
	END;

	COMMIT TRANSACTION
	GOTO EndSave
	QuitWithRollback:
		IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
	EndSave:
END;
GO




[CmdletBinding()]
Param(
   [Parameter()]
   [Alias("subscriptionID")]
   [string]$global:subscriptionID
)

function CheckStringInFile([string]$fileName, [string]$wordToFind){
	$file = Get-Content $fileName
	$containsWord = $file | %{$_ -match $wordToFind}
	If($containsWord -contains $true)
	{
		return "true"
	}
	return "false"	
}

function CheckExist([string]$sqlFile, [string]$logFile, [string]$SqlServer, [string]$localServer, [string]$dbName, [string]$userName, [string]$passWord, [string]$wordToFind){
	#check to see if this server, database, user existing
	
	if($localServer -eq 'Yes')
	{	
		sqlcmd.exe -S $SqlServer -E -i $sqlFile -v DBName=$dbName -o $logFile 	
	}
	else
	{
		sqlcmd.exe -S $SqlServer -U $userName -P $passWord -i $sqlFile -v DBName=$dbName -o $logFile 			
	}
	$wordExist = CheckStringInFile $logFile $wordToFind
	if($wordExist -eq "true")
	{
		return "false"
	}
	return "true"
}
	
#start of main script
$storePreference = $Global:VerbosePreference
	
$Global:VerbosePreference = "SilentlyContinue"
$setupDate = ((get-date).ToUniversalTime()).ToString("yyyy-MM-dd HH:mm:ss")
$setupDate2 = ((get-date).ToUniversalTime()).ToString("yyyyMMddHHmmss")
Write-Host "Deploy Start Date = $setupDate ..."
write-host "$PSScriptRoot"

$path = $PSScriptRoot + "\\logs\\" + $setupDate2
if (-Not (Test-Path  ($path)))	
{	
	New-Item -ItemType directory -force -Path $path | out-null
}

$global:logFile = $path + "\\setup.log"

echo "Setup Logs" > $global:logfile
echo "Deploy Start Date = $setupDate" > $global:logfile
echo "-------------------------------------------------------" >> $global:logfile

$dbConnection="Failed"
$SqlServer = Read-Host -Prompt 'Input Sql server name'

while($SqlServer -eq "")
{
	$SqlServer = Read-Host -Prompt 'Input Sql server name'
}

$dbName = Read-Host -Prompt 'Input Database Name'
while($dbName -eq "")
{
	$dbName = Read-Host -Prompt 'Input Database Name'
}	
$userName = Read-Host -Prompt 'Input Username'
while($userName -eq "")
{
	$userName = Read-Host -Prompt 'Input Username'
}	
$passWord = Read-Host -Prompt 'Input Password'
while($passWord -eq "")
{
	$passWord = Read-Host -Prompt 'Input Password'
}

#check to see if this server existing
write-host "Checking the server existing or not ..." -ForegroundColor White
$sqlFile = $PSScriptRoot + "\src\sql\Check_Server.sql"	
$logFile = $path + "\\check_server_exist.log"	
$serverExist1 = CheckExist $sqlFile $logFile $SqlServer $localServer $dbName $userName $passWord "Could not open a connection to SQL Server"
$serverExist2 = CheckExist $sqlFile $logFile $SqlServer $localServer $dbName $userName $passWord "A connection attempt failed"
while(($serverExist1 -ne "true") -Or ($serverExist2 -ne "true"))
{
	write-host "The server doest NOT exist, please make sure it exists and re-input" -ForegroundColor Red
	$SqlServer = Read-Host -Prompt 'Input Sql server name'
	$serverExist1 = CheckExist $sqlFile $logFile $SqlServer $localServer $dbName $userName $passWord "Could not open a connection to SQL Server"
	$serverExist2 = CheckExist $sqlFile $logFile $SqlServer $localServer $dbName $userName $passWord "A connection attempt failed"
}	
write-host "The server exists" -ForegroundColor Green

#check to see if this database existing
write-host "Checking the database existing or not ..." -ForegroundColor White
$sqlFile = $PSScriptRoot + "\src\sql\Check_Database.sql"	
$logFile = $path + "\\check_db_exist.log"	
$dbExist = CheckExist $sqlFile $logFile $SqlServer $localServer $dbName $userName $passWord "does not exist"
$loginSucceed = CheckExist $sqlFile $logFile $SqlServer $localServer $dbName $userName $passWord "Login failed for user"	

while($dbExist -ne "true")
{
	write-host "The database doest NOT exist, please make sure it exists and re-input" -ForegroundColor Red
	$dbName = Read-Host -Prompt 'Input Database Name'
	$dbExist = CheckExist $sqlFile $logFile $SqlServer $localServer $dbName $userName $passWord "does not exist"
}
while($loginSucceed -ne "true")
{
	write-host "The login to server and database failed, please make sure they are correct and re-input" -ForegroundColor Red
	$dbName = Read-Host -Prompt 'Input Database Name'
	$userName = Read-Host -Prompt 'Input Username'
	$passWord = Read-Host -Prompt 'Input Password'
	$dbExist = CheckExist $sqlFile $logFile $SqlServer $localServer $dbName $userName $passWord "does not exist"
}	
write-host "The database exists. We recommend that you have an empty database, otherwise the same tables and other same database objects for this demo will be wiped off " -ForegroundColor Yellow		

#create database objects
$logFile = $path + "\MRSSqlDB_creation.log"	

write-host "creating tables and other database objects ..." -ForegroundColor white		

$sqlFile = $PSScriptRoot + "\src\sql\MRSSqlDB_creation.sql"	
sqlcmd.exe -S $SqlServer -U $userName -P $passWord -i $sqlFile -v DBName=$dbName -o $logFile  	

$wordExist1 = CheckStringInFile $logFile "Cannot drop"
$wordExist2 = CheckStringInFile $logFile "already an object named"

if(($wordExist1 -eq "true") -Or ($wordExist2 -eq "true"))
{
	write-host "Errors when create tables and other database objects, please chech log file $logFile" -ForegroundColor Red
	return
}

write-host "Successfully created tables and other objects" -ForegroundColor Green	

#bulk load seed data to two tables

$DBtableDemand = "$dbName.dbo.DemandSeed"
$DBtableTemperature = "$dbName.dbo.TemperatureSeed"
$demandSeedFile = $PSScriptRoot + "\src\seeddata\DemandHistory15Minutes.txt"	
$temperatureSeedFile = $PSScriptRoot + "\src\seeddata\TemperatureHistoryHourly.txt"	

write-host "Bulk loading seed data into tables..." -ForegroundColor white	
bcp.exe $DBtableDemand IN $demandSeedFile -S $SqlServer -U $userName -P $passWord -c -h TABLOCK -b 100000 2>&1 3>&1 4>&1 1>>$global:logfile
bcp.exe $DBtableTemperature IN $temperatureSeedFile -S $SqlServer -U $userName -P $passWord -c -h TABLOCK -b 100000 2>&1 3>&1 4>&1 1>>$global:logfile

write-host "Successfully loaded seed data into tables" -ForegroundColor Green	

#call stored procedure to generate history data
write-host "Generating historical data from seed data ..." -ForegroundColor white	
$sqlFile = $PSScriptRoot + "\src\sql\MRSSqlDB_GenerateHistorialData.sql"
$logFile = $path + "\MRSSqlDB_GenerateHistorialData.log"

sqlcmd.exe -S $SqlServer -U $userName -P $passWord  -i $sqlFile -v DBName=$dbName -o $logFile  	
write-host "Successfully generated historical data" -ForegroundColor Green	

#create sql schedule job
$sqlFile = $PSScriptRoot + "\src\sql\MRSSqlDB_create_job.sql"	
$logFile = $path + "\MRSSqlDB_create_job.log"	

write-host "Scheduling jobs for data simulator which will run every 15 minutes to generate Demand data and run hourly to generate Temperature data from seed data ..." -ForegroundColor white	
if($SqlServer.contains(","))
{
	$server = ($SqlServer.Split(","))[0]
	$port = ($SqlServer.Split(","))[1]
	sqlcmd.exe -S $SqlServer -U $userName -P $passWord -i $sqlFile -v Servername = $server -v Port =$port -v DBName = $dbName -v Username = $userName -v Pswd = $passWord -o $logFile  	
}
else
{
	$server = $SqlServer
	$port = ""
	sqlcmd.exe -S $SqlServer -U $userName -P $passWord -i $sqlFile -v Servername = $server -v Port =$port -v DBName = $dbName -v Username = $userName -v Pswd = $passWord -o $logFile  	
}

$setupDate = ((get-date).ToUniversalTime()).ToString("yyyy-MM-dd HH:mm:ss")
Write-Host "Deploy Completed Date = $setupDate" -ForegroundColor Green

$Global:VerbosePreference = $storePreference







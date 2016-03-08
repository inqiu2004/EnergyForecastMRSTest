USE $(DBName)
GO
print '$(DBName)'

exec usp_delete_job '$(DBName)'
GO

print "$(ServerName)"
print "$(Port)"
declare @server varchar(100)

if ("$Port" <> "")
	set @server = concat('$(ServerName)',",", '$(Port)')
else
	set @server = '$(ServerName)'

exec usp_create_job @server, '$(DBName)', '$(UserName)', '$(Pswd)' 
GO



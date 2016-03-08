USE $(DBName)
GO
print '$(DBName)'
exec usp_delete_job '$(DBName)'
GO

exec usp_create_job '$(ServerName)', '$(DBName)', '$(UserName)', '$(Pswd)' 
GO



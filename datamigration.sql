USE [test]
GO
/****** Object:  StoredProcedure [dbo].[DataMigration]    Script Date: 31-05-2018 09:49:08 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER procedure [dbo].[DataMigration] 
as
begin
	declare @table varchar(100)
	declare @previousTable varchar(100)
	declare @day varchar(2)
	declare @maxdate datetime
	declare @sqlqry nvarchar(4000)
	declare @locationId smallint
	declare @deviceId smallint

	exec sp_addlinkedserver  @server='192.168.0.50'
	EXEC sp_addlinkedsrvlogin 'xxx.xxx.x.xx', 'false', NULL, 'username', 'password'

	set @table = 'DeviceLogs_' + cast(month(getdate()) as varchar(2)) + '_' + cast(year(getdate()) as varchar(4))
	set @previousTable = 'DeviceLogs_' + cast(month(DATEADD(month, -1, GETDATE()) ) as varchar(2)) + '_' + cast(year(DATEADD(month, -1, GETDATE())) as varchar(4))
	set @day = day(getdate())
	

	declare DataCursor cursor for 
		select LocationId, DeviceId from openquery([xxx.xxx.x.xx], select LocationId from etimetracklite1.dbo.Devices)	
		
	open DataCursor
	fetch next from DataCursor into @locationId, @deviceId

	while @@fetch_status = 0
		begin	
		
		if @day <= 5
			begin
				set @maxdate = (select isnull(MAX(pdatetime), '1900-01-01 00:00:00') as maxDate from 
				importdatahistory where month(pdatetime) = month(DATEADD(month, -1, GETDATE()))
				 and year(pdatetime) = year(DATEADD(month, -1, GETDATE())) and LocationId = @locationId)

				set @sqlqry = ('insert into importdatahistory (EmpCode, pDate, pDateTime, LocationId)
				select * from openquery([xxx.xxx.x.xx],
				''
				select E.EmployeeCode EmpCode, DL.DownloadDate pDate,
					DL.LogDate pDateTime, D.DeviceLocation  LocationId
				from etimetracklite1.dbo.Employees E inner join etimetracklite1.dbo.' + @previousTable + '
					DL on E.EmployeeCodeIndevice = DL.UserId 
				inner join etimetracklite1.dbo.Devices D on DL.DeviceId = D.DeviceId
				where DL.DeviceId = ' + @deviceId + ' and 
				convert(varchar(20), DL.LogDate, 120) > ''''' + convert(varchar(20), @maxdate, 120) + ''''''')')

				exec sp_executesql @sqlqry
			end 
				
			set @maxdate = (select isnull(MAX(pdatetime), '1900-01-01 00:00:00') as maxDate from 
			importdatahistory where month(pdatetime) = month(GETDATE())
				and year(pdatetime) = year(GETDATE()) and LocationId = @locationId)

			set @sqlqry = ('insert into importdatahistory (EmpCode, pDate, pDateTime, LocationId)
			select * from openquery([xxx.xxx.x.xx],
			''
			select E.EmployeeCode EmpCode, DL.DownloadDate pDate,
				DL.LogDate pDateTime, D.DeviceLocation  LocationId
			from etimetracklite1.dbo.Employees E inner join etimetracklite1.dbo.' + @table + '
				DL on E.EmployeeCodeIndevice = DL.UserId 
			inner join etimetracklite1.dbo.Devices D on DL.DeviceId = D.DeviceId
			where DL.DeviceId = ' + @deviceId + ' and 
			convert(varchar(20), DL.LogDate, 120) > ''''' + convert(varchar(20), @maxdate, 120) + ''''''')')

			--print @sqlqry
			exec sp_executesql @sqlqry
			
			fetch next from DataCursor into @locationId, @deviceId
		end
	close DataCursor
	deallocate DataCursor

	EXEC sp_dropserver 'xxx.xxx.x.xx', 'droplogins';

end
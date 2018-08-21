USE [master]
GO

/****** Object:  StoredProcedure [dbo].[pbackup_q_mod]    Script Date: 21.08.2018 16:23:47 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



ALTER PROCEDURE [dbo].[pbackup_q_mod] 
	(@DB VARCHAR(100), -- имя БД
	 @TYPE VARCHAR(10) = 'FULL') -- адрес получателя
AS
BEGIN
	DECLARE @DT1 VARCHAR(20),      -- время начала
	@DT2 VARCHAR(20),              -- время окончания
	@DRUN VARCHAR(10),             -- время выполнения
	@NAMEBACKUP VARCHAR(200),      -- лог.имя устройства бэкапа
	@MESS VARCHAR(4000),           -- строка с HTML 
	@SUBJ VARCHAR(300),            -- текст темы 
	@SRV VARCHAR(30),              -- сервер
	@From VARCHAR(100),            -- адрес отправителя 
	@STR_SQL VARCHAR(4000),        -- запрос
	@FL_FULL INT,                  -- флаг признака создания полной резервной копии
	@FL_DIFF INT,                  -- флаг признака создания дифференциальной резервной копии
	@FL_LOG INT,                   -- флаг признака создания журнала транз.логов
	@OUTPUT_FILE_NAME VARCHAR(200),-- выходной файл, который определен в шаге job
	@STATUS VARCHAR(10),           -- статус для строки в таблице master..backup_q
	@FILEBACKUP VARCHAR(300),      -- физический путь бэкапа
	@CMD sysname,                  -- командная строка
	@ERROR	INT,				   -- код ошибки	
	@ERROR_VERIFYONLY	INT,	   -- код ошибки (RESTORE VERIFYONLY)
	@ERROR_HEADERONLY	INT,	   -- код ошибки (RESTORE HEADERONLY)	
	@File_Exists INT,
	@DB_OBZ VARCHAR(100),		   -- имя обезличиваемой БД
	@IS_CLUST INT,				   -- признак, что экземпляр SQL-сервера кластерный
	@NAME_INSTANCE VARCHAR(20),	   -- название экземляра SQL-сервера
	@NAME_SQLSERVER VARCHAR (100), -- название SQL-сервера
	@srv_backup	varchar(200), 	   -- сервер бэкапов
	@name_pool varchar(100),	   -- имя пула (ленточный или дисковый)
	@need_restore INT,			   -- признак, что БД нужно будет в дальнейшем восстанавливать
	@tmp_str VARCHAR(1000)
-- 
SET NOCOUNT ON

set @srv_backup = 'backup9.qiwi.com'
--set @name_pool = 'DL_m1_3par8440_1week'

SELECT @DB_OBZ = @DB+'_OBZ'

-- Имя сервера
SELECT @SRV = CAST(SERVERPROPERTY('servername') AS VARCHAR(100))

-- Определение файла, в который выполняется вывод команд T_SQL из текущего шага job
BEGIN TRY
	select @OUTPUT_FILE_NAME = b.output_file_name from msdb..sysjobs a 
	JOIN msdb..sysjobsteps b
	on a.job_id = b.job_id 
	where a.job_id = master.dbo.fn_currentJobId()
END TRY
BEGIN CATCH
	SET @OUTPUT_FILE_NAME = ''
END CATCH

--- Анализируем: кластерный экземпляр, имя экземпляра, имя sql сервера
SELECT @IS_CLUST = CAST(SERVERPROPERTY('IsClustered') as INT), 
		  			@NAME_SQLSERVER = CASE SERVERPROPERTY('IsClustered')
						WHEN 1 THEN CAST(SERVERPROPERTY('MachineName') as VARCHAR(100)) ELSE CAST(SERVERPROPERTY('Servername') as VARCHAR(100)) END,
						@NAME_INSTANCE = CAST(SERVERPROPERTY('InstanceName') as VARCHAR(12))

CREATE TABLE #Tmp_Error (RC BIT NULL)		--- таблица, в которой хранится результат вызова xp_cmdshell
CREATE TABLE #Temp (str_f VARCHAR (2000))	--- таблица, в которой хранится журнал команды nsrsqlrc

IF @TYPE = 'FULL' 
BEGIN
	SELECT @NAMEBACKUP = BACKUPNAME, @name_pool = NAME_POOL, @FL_FULL = FLAG, @need_restore = NEED_RESTORE FROM master.dbo.backup_q Where DBName =  @DB and BackupType = @Type
	BEGIN TRANSACTION -- процесс создания полной копии - длительный процесс, поэтому сразу коммитим изменения в таблице с флагами
		--- сбрасываем все флаги и строим новую цепочку восстановления
		UPDATE master.dbo.backup_q SET flag = 0, statusrun = 'NEW_CHAIN' WHERE DBName =  @DB
    COMMIT TRANSACTION
	--- делаем полный бэкап

	-- Формируем строку для выполнения бэкапа	
	SET @STR_SQL = 'nsrsqlsv.exe -s '+@srv_backup
	
	IF @IS_CLUST = 1
	BEGIN
		SET @STR_SQL = @STR_SQL + ' -c '+@NAME_SQLSERVER+' -A '+@NAME_SQLSERVER
	END
--	SET @STR_SQL = @STR_SQL +' -l full -X -q -b "'+@name_pool+'" "MSSQL'
	SET @STR_SQL = @STR_SQL +' -l full -X -b "'+@name_pool+'" "MSSQL'
	IF @IS_CLUST = 0 and @NAME_INSTANCE IS NOT NULL
	BEGIN
		SET @STR_SQL = @STR_SQL +'$'+@NAME_INSTANCE
	END
	SET @STR_SQL = @STR_SQL +':'+@DB+'"'

	PRINT '<----LOG---->'+@STR_SQL+CHAR(13)+CHAR(10)
	
	SET @STR_SQL = '
	DECLARE @Result BIT
	EXECUTE @Result = master..xp_cmdshell ''' + @STR_SQL + '''
	INSERT INTO #Tmp_Error SELECT @Result'
	INSERT #Temp EXECUTE(@STR_SQL)

--	select * from #Temp
	select @tmp_str = substring(str_f, patindex('%EMC#%', str_f), 40) from #Temp where str_f is not NULL and str_f like '%EMC#%'
--	print @tmp_str
	select @error = rc from #Tmp_Error

--	EXEC @ERROR = xp_cmdshell @STR_SQL;  

	IF @ERROR=0
	BEGIN
		--- логируем сообщения (для теста)
		PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
		' - успешно выполнен полный бэкап , начинаем новую цепочку восстановления '+CHAR(13)+CHAR(10)			
		UPDATE master.dbo.backup_q SET flag = 1, LASTRUN = GETDATE(), statusrun = 'FULL_OK' WHERE DBName =  @DB AND BackupType = @Type
		-- Отправка сообщения об успешном выполнении полного бэкапа
        EXEC master..pbackup_list_networker @DB = @DB, @physical_device_name = @tmp_str, @type_backup = @TYPE
		EXEC master..backup_info_q_mod @DB = @DB

-------------------------------------------------------------------------------------------------------------------------------------------------------------
---- добавлено 29.01.2014 для выставления признака, что бэкап БД Contact выполнен
---- изменено 03.02.2014 для восстановления на сервер MDM
-------------------------------------------------------------------------------------------------------------------------------------------------------------
--		DECLARE @mess VARCHAR(4000)
		IF @need_restore = 1
			BEGIN
			SET @MESS = 'sqlcmd -S NG-PDSQLNGT1 -E -d master -q "update master.dbo.backup_db set backup_on_source=1, createdate_backup = getdate() where dbname = '''+@db+'''"'
			PRINT @MESS
			EXEC xp_cmdshell @MESS

			SET @MESS = 'sqlcmd -S NG-PDSQLNGT2 -E -d master -q "update master.dbo.backup_db set backup_on_source=1, createdate_backup = getdate() where dbname = '''+@db+'''"'
			PRINT @MESS
			EXEC xp_cmdshell @MESS
		END
-------------------------------------------------------------------------------------------------------------------------------------------------------------
---- добавлено 13.11.2014 для выставления признака, что бэкап БД Contact выполнен и можно начинать восстановление в БД, которая затем будет обезличена Contact_Obz
-------------------------------------------------------------------------------------------------------------------------------------------------------------
--		DECLARE @mess VARCHAR(4000)
        --- ставим паузу в 1 минуту, чтобы не было конфликтов при запуске job на mdm 
/*		WAITFOR DELAY '00:01:00'
        SET @MESS = 'sqlcmd -S MDM -E -d master -q "update master.dbo.backup_db set backup_on_source=1, createdate_backup = getdate() where dbname = '''+@DB_OBZ+'''"'
        EXEC xp_cmdshell @MESS
*/
	END
	ELSE
	BEGIN
		UPDATE master.dbo.backup_q SET flag = 0, LASTRUN = GETDATE(), statusrun = 'FULL_ERR' WHERE DBName =  @DB AND BackupType = @Type
		UPDATE master.dbo.backup_q SET flag = 1, LASTRUN = GETDATE(), statusrun = 'CONT_CHAIN' WHERE DBName =  @DB AND BackupType <> @Type
		--- логируем сообщения (для теста)
		PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
		'Полный бэкап завершился с ошибкой. Код ошибки - '+CAST(@ERROR AS VARCHAR(10))+', продолжаем старую цепочку восстановления'+
		+CHAR(13)+CHAR(10)
		--- отправка сообщения об ошибке
		SET @MESS = @SRV +' '+@DB+' '+@Type+' - FAIL, См. '+@OUTPUT_FILE_NAME          
		EXEC master..sp_send_email_fail @message = @mess, @At= @OUTPUT_FILE_NAME
		EXEC master..sp_send_email_fail @message = @mess, @Recipient = 'storage@qiwi.com', @At= @OUTPUT_FILE_NAME
		EXEC master..sp_send_email_fail @message = @mess, @Recipient = 'ss@qiwi.com', @At= @OUTPUT_FILE_NAME
--		EXEC master..sp_send_email_fail @message = @mess, @Recipient = 'dba@qiwi.com', @At= @OUTPUT_FILE_NAME
	END  
	if object_id('tempdb..#Temp') is not null
		drop table #Temp
END

IF @TYPE = 'DIFF' 
BEGIN
	SELECT @FL_FULL = FLAG FROM master.dbo.backup_q Where DBName =  @DB and BackupType = 'FULL'
	SELECT @NAMEBACKUP = BACKUPNAME, @name_pool = NAME_POOL, @FL_DIFF = FLAG, @need_restore = NEED_RESTORE FROM master.dbo.backup_q Where DBName =  @DB and BackupType = @Type

	
	IF ((@FL_FULL = 1) and (@FL_Diff = 0)) OR (@FL_Diff = 1) 
	BEGIN

		-- Формируем строку для выполнения бэкапа	
		SET @STR_SQL = 'nsrsqlsv.exe -s '+@srv_backup
		
		IF @IS_CLUST = 1
		BEGIN
			SET @STR_SQL = @STR_SQL + ' -c '+@NAME_SQLSERVER+' -A '+@NAME_SQLSERVER
		END
--		SET @STR_SQL = @STR_SQL +' -l diff -X -q -b "'+@name_pool+'" "MSSQL'
		SET @STR_SQL = @STR_SQL +' -l diff -X -b "'+@name_pool+'" "MSSQL'
		IF @IS_CLUST = 0 and @NAME_INSTANCE IS NOT NULL
		BEGIN
			SET @STR_SQL = @STR_SQL +'$'+@NAME_INSTANCE
		END
		SET @STR_SQL = @STR_SQL +':'+@DB+'"'


		IF (@FL_FULL = 1) and (@FL_Diff = 0) -- создание 1-й дифф.копии
		BEGIN
			SET @STATUS = 'FIRST_DIFF'
		END	
		IF @FL_Diff = 1 -- создание 2-й и последующих дифф.копий
		BEGIN
			SET @STATUS = 'NEXT_DIFF'
		END	
		
		SET @STR_SQL = '
		DECLARE @Result BIT
		EXECUTE @Result = master..xp_cmdshell ''' + @STR_SQL + '''
		INSERT INTO #Tmp_Error SELECT @Result'
		INSERT #Temp EXECUTE(@STR_SQL)

--		select * from #Temp
		select @tmp_str = substring(str_f, patindex('%EMC#%', str_f), 40) from #Temp where str_f is not NULL and str_f like '%EMC#%'
--		print @tmp_str
		select @error = rc from #Tmp_Error


--		EXEC @ERROR = xp_cmdshell @STR_SQL;  

		IF @ERROR=0
		BEGIN
			--- логируем сообщения (для теста)
			IF @STATUS = 'FIRST_DIFF'
			BEGIN
				PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
				' - успешно выполнен 1-й дифференциальный бэкап'+CHAR(13)+CHAR(10)
			END 
			IF @STATUS = 'NEXT_DIFF'
			BEGIN
				PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
				' - успешно выполнен следующий дифференциальный бэкап'+CHAR(13)+CHAR(10)
			END 			
			UPDATE master.dbo.backup_q SET flag = 1, LASTRUN = GETDATE(), statusrun = @STATUS WHERE DBName =  @DB AND BackupType = @Type
	        EXEC master..pbackup_list_networker @DB = @DB, @physical_device_name = @tmp_str, @type_backup = @TYPE

		END 
		ELSE
		BEGIN
			PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
			'Дифф. бэкап завершился с ошибкой. Код ошибки - '+CAST(@ERROR AS VARCHAR(10))+CHAR(13)+CHAR(10)
			UPDATE master.dbo.backup_q SET flag = 1, LASTRUN = GETDATE(), statusrun = 'DIFF_ERR' WHERE DBName =  @DB AND BackupType = @Type
			SET @MESS = @SRV +' '+@DB+' '+@Type+' - FAIL, См. '+@OUTPUT_FILE_NAME          
			EXEC master..sp_send_email_fail @message = @mess, @At= @OUTPUT_FILE_NAME	
			EXEC master..sp_send_email_fail @message = @mess, @Recipient = 'storage@qiwi.com', @At= @OUTPUT_FILE_NAME
			EXEC master..sp_send_email_fail @message = @mess, @Recipient = 'ss@qiwi.com', @At= @OUTPUT_FILE_NAME
--			EXEC master..sp_send_email_fail @message = @mess, @Recipient = 'dba@qiwi.com', @At= @OUTPUT_FILE_NAME
		END				
	END
	ELSE
	BEGIN
		PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
		' - ожидание выполнения полного бэкапа...'+CHAR(13)+CHAR(10)	
	END
	if object_id('tempdb..#Temp') is not null
		drop table #Temp
END
IF @TYPE = 'LOG' 
BEGIN
	SELECT @FL_FULL = FLAG FROM master.dbo.backup_q Where DBName =  @DB and BackupType = 'FULL'
	SELECT @NAMEBACKUP = BACKUPNAME, @name_pool = NAME_POOL, @FL_LOG = FLAG, @need_restore = NEED_RESTORE FROM master.dbo.backup_q Where DBName =  @DB and BackupType = @Type

	IF ((@FL_FULL = 1) and (@FL_LOG = 0)) OR (@FL_LOG = 1)
	BEGIN

		-- Формируем строку для выполнения бэкапа	
		SET @STR_SQL = 'nsrsqlsv.exe -s '+@srv_backup
		
		IF @IS_CLUST = 1
		BEGIN
			SET @STR_SQL = @STR_SQL + ' -c '+@NAME_SQLSERVER+' -A '+@NAME_SQLSERVER
		END
--		SET @STR_SQL = @STR_SQL +' -l txnlog -X -q -b "'+@name_pool+'" "MSSQL'
		SET @STR_SQL = @STR_SQL +' -l txnlog -X -b "'+@name_pool+'" "MSSQL'
		IF @IS_CLUST = 0 and @NAME_INSTANCE IS NOT NULL
		BEGIN
			SET @STR_SQL = @STR_SQL +'$'+@NAME_INSTANCE
		END
		SET @STR_SQL = @STR_SQL +':'+@DB+'"'
			
		IF (@FL_FULL = 1) and (@FL_LOG = 0) -- создание 1-й копии журнала лога
		BEGIN
			-- скрипт создания 1-го журнала лога
			SET @STATUS = 'FIRST_LOG'
		END	
		IF @FL_LOG = 1 -- создание 2-й и последующих копий журнала лога
		BEGIN
			SET @STATUS = 'NEXT_LOG'
		END		
		
		SET @STR_SQL = '
		DECLARE @Result BIT
		EXECUTE @Result = master..xp_cmdshell ''' + @STR_SQL + '''
		INSERT INTO #Tmp_Error SELECT @Result'
		INSERT #Temp EXECUTE(@STR_SQL)

--		select * from #Temp
		select @tmp_str = substring(str_f, patindex('%EMC#%', str_f), 40) from #Temp where str_f is not NULL and str_f like '%EMC#%'
--		print @tmp_str
		select @error = rc from #Tmp_Error

--		EXEC @ERROR = xp_cmdshell @STR_SQL;  
			
		IF @ERROR=0
		BEGIN
			IF @STATUS = 'FIRST_LOG'
			BEGIN
				PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
				' - успешно выполнен 1-й бэкап логов'+CHAR(13)+CHAR(10)
			END 
			IF @STATUS = 'NEXT_LOG'
			BEGIN
				PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
				' - успешно выполнен следующий бэкап логов'+CHAR(13)+CHAR(10)
			END 		
			UPDATE master.dbo.backup_q SET flag = 1, LASTRUN = GETDATE(), statusrun = @STATUS WHERE DBName =  @DB AND BackupType = @Type
	        EXEC master..pbackup_list_networker @DB = @DB, @physical_device_name = @tmp_str, @type_backup = @TYPE
		END 
		ELSE
		BEGIN
			PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
			'Бэкап логов завершился с ошибкой. Код ошибки - '+CAST(@ERROR AS VARCHAR(10))+CHAR(13)+CHAR(10)
			UPDATE master.dbo.backup_q SET flag = 1, LASTRUN = GETDATE(), statusrun = 'LOG_ERR' WHERE DBName =  @DB AND BackupType = @Type
			SET @MESS = @SRV +' '+@DB+' '+@Type+' - FAIL, См. '+@OUTPUT_FILE_NAME          
			EXEC master..sp_send_email_fail @message = @mess, @At= @OUTPUT_FILE_NAME	
			EXEC master..sp_send_email_fail @message = @mess, @Recipient = 'storage@qiwi.com', @At= @OUTPUT_FILE_NAME
			EXEC master..sp_send_email_fail @message = @mess, @Recipient = 'ss@qiwi.com', @At= @OUTPUT_FILE_NAME
--			EXEC master..sp_send_email_fail @message = @mess, @Recipient = 'dba@qiwi.com', @At= @OUTPUT_FILE_NAME
		END			
	END
	ELSE
	BEGIN
		PRINT '<----LOG---->'+CONVERT(VARCHAR(10),GETDATE(),104)+' '+CONVERT(VARCHAR(10),GETDATE(),108)+
		' - ожидание выполнения полного бэкапа...'+CHAR(13)+CHAR(10)	
	END

END
if object_id('tempdb..#Tmp_Error') is not null
	drop table #Tmp_Error
if object_id('tempdb..#Temp') is not null
	drop table #Temp
END








GO


SET QUOTED_IDENTIFIER, ANSI_NULLS ON;
GO
/**********************************************************************************************************************
** Description
**  This stored procedure exists to return results of column values that have changed from a Temporal table (also 
**  known as system-versioned temporal table). It generates dynamic SQL and either executes it or outputs it with the
**  debug parameter.
** 
** Supports
**  2016 and higher version (SQL Server, Azure SQL Database, Azure SQL Managed Instance) is required for the 
**  LAG(), OPENJSON, THROW functions.
**
** Notes
**  Columns data types Image & RowVersion will be ignored due casting error in comparison operation.
**
**  Grouping the output results in the user interface by the ChangeTime column would keep all the column changes 
**  that occurred at the same time together.
**
** Performance
**  If you do not set the @PrimaryKeyValue parameter, you will scan the entire current table.
**
**  An optimal indexing strategy will include a clustered columns store index and / or a B-tree rowstore index on the 
**  current table and a clustered columnstore index on the history table for optimal storage size and performance. 
**  If you create / use your own history table, it is strongly recommended that you create this type of index consisting of 
**  period columns starting with the end of period column, to speed up temporal querying and speed up the queries that 
**  are part of the data consistency check. The default history table has a clustered rowstore index created for you 
**  based on the period columns (end, start). At a minimum, a nonclustered rowstore index is recommended.
**
** Parameters
**  See comments to the right of the parameters below.
**  
** Portions of this code are part of project sp_CRUDGen and are provided under the MIT license:
** https://github.com/kevinmartintech/sp_CRUDGen 
**********************************************************************************************************************/
CREATE OR ALTER PROCEDURE dbo.TemporalTableChangesGet (
    @SchemaTableName                       nvarchar(257)                   /* This is a required parameter in the form of SchemaName.TableName */
   ,@PrimaryKeyValue                       nvarchar(MAX) = NULL            /* If you want to only see the table changes for a single record, you can pass in the PK value. */
   ,@RowUpdatePersonColumnName             nvarchar(128) = NULL            /* If the main table has a foreign key to a lookup/relation table, you can specify the FK column like RowModifyPersonId and the stored procedure will determine the table and create a JOIN to return the @RowUpdatePersonColumnNameValue parameter. */
   ,@RowUpdatePersonColumnNameValue        nvarchar(128) = NULL            /* This is the column name that stores the name of the person who changed the record. Uses would be a UserName or a computed column like FullName. This can be either on the main table or the lookup/relation table joined from the main table. */
   ,@IgnoreColumns                         nvarchar(MAX) = NULL            /* Multivalued JSON like ["FirstName","LastName","Title"]. Add columns you do not wish to be outputed in the results. */
   ,@MaskColumns                           nvarchar(MAX) = NULL            /* Multivalued JSON like ["SSN","DateOfBirth"]. Add columns you do not wish the values to be viewable in the output results. The results will be **** instead of the old and new values. */
   ,@FormatColumnNames                     bit           = 1               /* The default 1 will add a space before capital letters in the column names so "FirstName" will turn into "First Name". */
   ,@FormatColumnNamesPreserveAdjacentCaps bit           = 1               /* The default 1 will not add spaces when capital letters are next to each other so "TPSReport" will be "TPS Report". */
   ,@PrimaryKeyResultColumnName            nvarchar(MAX) = N'Identifier'   /* Change the default output result column name for the 'PrimaryKey' column */
   ,@ColumnNameResultColumnName            nvarchar(MAX) = N'Field Name'   /* Change the default output result column name for the 'ColumnName' column */
   ,@OldValueResultColumnName              nvarchar(MAX) = N'Old Value'    /* Change the default output result column name for the 'OldValue' column  */
   ,@NewValueResultColumnName              nvarchar(MAX) = N'New Value'    /* Change the default output result column name for the 'NewValue' column  */
   ,@ChangedByResultColumnName             nvarchar(MAX) = N'Changed By'   /* Change the default output result column name for the 'ChangedBy' column  */
   ,@ChangedTimeResultColumnName           nvarchar(MAX) = N'Changed Time' /* Change the default output result column name for the 'Changed Time' column  */
   ,@OrderBy                               nvarchar(4)   = N'DESC'         /* Use [ASC|DESC] to control the ORDER BY of the output results. */
   ,@Debug                                 bit           = 0               /* If you set this to 1 it will display an XML link in the results you can click on to view the query text. If you set this to 0 it will execute the query. */
)
AS
    BEGIN
        SET NOCOUNT, XACT_ABORT ON;

        /**********************************************************************************************************************
        ** Check if version is 2016 or greater to support LAG(), OPENJSON, THROW
        **********************************************************************************************************************/
        IF @@VERSION LIKE '%Microsoft SQL Server 2000%'
        OR @@VERSION LIKE '%Microsoft SQL Server 2005%'
        OR @@VERSION LIKE '%Microsoft SQL Server 2008%'
        OR @@VERSION LIKE '%Microsoft SQL Server 2012%'
        OR @@VERSION LIKE '%Microsoft SQL Server 2014%'
            BEGIN
                ; THROW 52001, 'SQL Server 2012 or greater is required!', 1;
            END;


        /**********************************************************************************************************************
        ** Check if OrderBy parameters are valid
        **********************************************************************************************************************/
        IF @OrderBy <> N'ASC'
        AND @OrderBy <> N'DESC'
            BEGIN
                ; THROW 52001, 'OrderBy parameter is not a valid! Can only be ASC or DESC.', 1;
            END;


        /**********************************************************************************************************************
        ** Declare varibles
        **********************************************************************************************************************/
        DECLARE @SeparatorStartingPosition int;
        DECLARE @SchemaName nvarchar(MAX);
        DECLARE @TableName nvarchar(MAX);
        DECLARE @TableAlias nvarchar(MAX);
        DECLARE @NewLineString nvarchar(MAX);
        DECLARE @ExecuteChangesGetString nvarchar(MAX);
        DECLARE @TemporalWithQueryColumns nvarchar(MAX);
        DECLARE @TemporalWithQueryWhere nvarchar(MAX);
        DECLARE @TemporalCrossApplyColumns nvarchar(MAX);
        DECLARE @PrimaryKeyTableColumn nvarchar(MAX);
        DECLARE @ValidFromTimeTableColumn nvarchar(MAX);
        DECLARE @SchemaName_Reference nvarchar(MAX);
        DECLARE @TableName_Reference nvarchar(MAX);
        DECLARE @ColumnName_Reference nvarchar(MAX);
        DECLARE @TableAlias_Reference nvarchar(MAX);
        DECLARE @ChangedByFrom nvarchar(MAX);
        DECLARE @MaskList AS nvarchar(MAX);
        DECLARE @ColumnListId int;
        DECLARE @ColumnNameRaw nvarchar(128);
        DECLARE @ColumnNameFormatted nvarchar(128);
        DECLARE @i int;
        DECLARE @ColumnNameRawLength int;
        DECLARE @PreviousCharacter nchar(1);
        DECLARE @CurrentCharacter nchar(1);
        DECLARE @NextCharacter nchar(1);

        /**********************************************************************************************************************
        ** Set varibles
        **********************************************************************************************************************/
        SET @NewLineString = CAST(CHAR(13) + CHAR(10) AS nvarchar(MAX));
        SET @TemporalWithQueryColumns = N'';
        SET @TemporalWithQueryWhere = N'';
        SET @TemporalCrossApplyColumns = N'';
        SET @ChangedByFrom = N'';
        SET @MaskList = N'';


        /**********************************************************************************************************************
        ** Create Temp Tables - This is for inserting JSON into for passing a list of parameter values
        **********************************************************************************************************************/
        CREATE TABLE #IgnoreColumns (ColumnName nvarchar(128) NULL);
        CREATE TABLE #MaskColumns (ColumnName nvarchar(128) NULL);


        /**********************************************************************************************************************
        ** Extract JSON array into temporary table for use with Exists and NotExists where operators
        **********************************************************************************************************************/
        INSERT INTO #IgnoreColumns (ColumnName)
        SELECT CAST(Value AS nvarchar(128))FROM OPENJSON(@IgnoreColumns);

        INSERT INTO #MaskColumns (ColumnName)
        SELECT CAST(Value AS nvarchar(128))FROM OPENJSON(@MaskColumns);


        /**********************************************************************************************************************
        ** Parse the passed in parameter
        **********************************************************************************************************************/
        IF @SchemaTableName IS NOT NULL
            BEGIN
                SET @SeparatorStartingPosition = CHARINDEX('.', @SchemaTableName);
                IF @SeparatorStartingPosition > 0
                    BEGIN
                        SELECT
                            @SchemaName = LEFT(@SchemaTableName, @SeparatorStartingPosition - 1)
                           ,@TableName  = RIGHT(@SchemaTableName, LEN(@SchemaTableName) - @SeparatorStartingPosition);
                    END;
                ELSE
                    BEGIN
                        SELECT @SchemaName = N'dbo', @TableName = @SchemaTableName;
                    END;
            END;


        /**********************************************************************************************************************
        ** Create and load the table list
        **********************************************************************************************************************/
        IF OBJECT_ID('tempdb..#TableList') IS NOT NULL
            BEGIN
                DROP TABLE #TableList;
            END;
        CREATE TABLE #TableList (
            TableListId                  int           NOT NULL IDENTITY(1, 1) PRIMARY KEY
           ,Depth                        int           NULL
           ,parent_object_id             int           NULL
           ,referenced_object_id         int           NULL
           ,referenced_schema            nvarchar(MAX) NULL
           ,referenced_table             nvarchar(MAX) NULL
           ,referenced_table_description nvarchar(MAX) NULL
           ,referenced_alias             nvarchar(MAX) NULL
           ,referenced_column            nvarchar(MAX) NULL
           ,parent_schema                nvarchar(MAX) NULL
           ,parent_table                 nvarchar(MAX) NULL
           ,parent_alias                 nvarchar(MAX) NULL
           ,parent_column                nvarchar(MAX) NULL
           ,parent_column_is_nullable    bit           NULL
           ,HasTriggers                  bit           NULL
           ,IsTemporal                   bit           NULL DEFAULT 0
           ,IsRowUpdateTable             bit           NULL DEFAULT 0
           ,IsProcessed                  bit           NULL
           ,HierarchyPath                nvarchar(MAX) NULL
        );


        /* Find main table */
        INSERT INTO #TableList (
            parent_object_id, referenced_object_id, Depth, referenced_schema, referenced_table
        )
        SELECT
            parent_object_id     = NULL
           ,referenced_object_id = T.object_id
           ,Depth                = 0
           ,referenced_schema    = S.name
           ,referenced_table     = T.name
        FROM
            sys.tables         AS T
        INNER JOIN sys.schemas AS S ON T.schema_id = S.schema_id
        WHERE
            T.name = @TableName
        AND S.name = @SchemaName;


        /**********************************************************************************************************************
        ** Find row updated by table
        **********************************************************************************************************************/
        IF @RowUpdatePersonColumnName IS NOT NULL
            BEGIN

                INSERT INTO #TableList (
                    IsRowUpdateTable, referenced_schema, referenced_table, referenced_column
                )
                SELECT
                    IsRowUpdateTable  = 1
                   ,referenced_schema = SR.name
                   ,referenced_table  = TR.name
                   ,referenced_column = CR.name
                FROM
                    sys.columns                    AS C
                INNER JOIN sys.tables              AS T ON C.object_id                = T.object_id
                INNER JOIN sys.schemas             AS S ON T.schema_id                = S.schema_id
                INNER JOIN sys.foreign_key_columns AS FKC ON T.object_id              = FKC.parent_object_id
                                                          AND C.column_id             = FKC.parent_column_id
                INNER JOIN sys.tables              AS TR ON FKC.referenced_object_id  = TR.object_id
                INNER JOIN sys.schemas             AS SR ON TR.schema_id              = SR.schema_id
                INNER JOIN sys.columns             AS CR ON FKC.referenced_object_id  = CR.object_id
                                                         AND FKC.constraint_column_id = CR.column_id
                WHERE
                    C.name = @RowUpdatePersonColumnName
                AND T.name = @TableName
                AND S.name = @SchemaName;
            END;


        /* Perform Numbering */
        WITH Numbering
          AS (
              SELECT
                  TableListId               = TL.TableListId
                 ,parent_object_id          = TL.parent_object_id
                 ,referenced_object_id      = TL.referenced_object_id
                 ,Depth                     = TL.Depth
                 ,referenced_schema         = TL.referenced_schema
                 ,referenced_table          = TL.referenced_table
                 ,referenced_alias          = CAST(CASE WHEN LEN(R.referenced_table) > 0
                                                            THEN R.referenced_table
                                                       ELSE UPPER(LEFT(TL.referenced_table, 1))
                                                   END
                                                   + CASE WHEN CAST(ROW_NUMBER() OVER (PARTITION BY
                                                                                           CASE WHEN LEN(R.referenced_table) > 0
                                                                                                    THEN R.referenced_table
                                                                                               ELSE
                                                                                                   UPPER(
                                                                                                       LEFT(TL.referenced_table, 1)
                                                                                                   )
                                                                                           END
                                                                                       ORDER BY
                                                                                           TL.Depth ASC
                                                                                 ) AS nvarchar(MAX)) = 1
                                                              THEN CAST(N'' AS nvarchar(MAX))
                                                         ELSE
                                                             CAST(ROW_NUMBER() OVER (PARTITION BY
                                                                                         CASE WHEN LEN(R.referenced_table) > 0
                                                                                                  THEN R.referenced_table
                                                                                             ELSE
                                                                                                 UPPER(
                                                                                                     LEFT(TL.referenced_table, 1)
                                                                                                 )
                                                                                         END
                                                                                     ORDER BY
                                                                                         TL.Depth ASC
                                                                               ) - 1 AS nvarchar(MAX))
                                                     END AS nvarchar(MAX))
                 ,referenced_column         = TL.referenced_column
                 ,parent_schema             = TL.parent_schema
                 ,parent_table              = TL.parent_table
                 ,parent_column             = TL.parent_column
                 ,parent_column_is_nullable = TL.parent_column_is_nullable
                 ,HierarchyPath             = TL.HierarchyPath
              FROM
                    -- SQL Prompt formatting off
                    #TableList  AS TL 
                    CROSS APPLY (SELECT REPLACE(TL.referenced_table COLLATE Latin1_General_BIN, 'a', N'')  AS referenced_table) AS R1 /* TRANSLATE() would work with one line but is only supported in 2017+ */
                    CROSS APPLY (SELECT REPLACE(R1.referenced_table  COLLATE Latin1_General_BIN, 'b', N'')  AS referenced_table) AS R2
                    CROSS APPLY (SELECT REPLACE(R2.referenced_table  COLLATE Latin1_General_BIN, 'c', N'')  AS referenced_table) AS R3
                    CROSS APPLY (SELECT REPLACE(R3.referenced_table  COLLATE Latin1_General_BIN, 'd', N'')  AS referenced_table) AS R4
                    CROSS APPLY (SELECT REPLACE(R4.referenced_table  COLLATE Latin1_General_BIN, 'e', N'')  AS referenced_table) AS R5
                    CROSS APPLY (SELECT REPLACE(R5.referenced_table  COLLATE Latin1_General_BIN, 'f', N'')  AS referenced_table) AS R6
                    CROSS APPLY (SELECT REPLACE(R6.referenced_table  COLLATE Latin1_General_BIN, 'g', N'')  AS referenced_table) AS R7
                    CROSS APPLY (SELECT REPLACE(R7.referenced_table  COLLATE Latin1_General_BIN, 'h', N'')  AS referenced_table) AS R8
                    CROSS APPLY (SELECT REPLACE(R8.referenced_table  COLLATE Latin1_General_BIN, 'i', N'')  AS referenced_table) AS R9
                    CROSS APPLY (SELECT REPLACE(R9.referenced_table  COLLATE Latin1_General_BIN, 'j', N'')  AS referenced_table) AS R10
                    CROSS APPLY (SELECT REPLACE(R10.referenced_table COLLATE Latin1_General_BIN, 'k', N'')  AS referenced_table) AS R11
                    CROSS APPLY (SELECT REPLACE(R11.referenced_table COLLATE Latin1_General_BIN, 'l', N'')  AS referenced_table) AS R12
                    CROSS APPLY (SELECT REPLACE(R12.referenced_table COLLATE Latin1_General_BIN, 'm', N'')  AS referenced_table) AS R13
                    CROSS APPLY (SELECT REPLACE(R13.referenced_table COLLATE Latin1_General_BIN, 'n', N'')  AS referenced_table) AS R14
                    CROSS APPLY (SELECT REPLACE(R14.referenced_table COLLATE Latin1_General_BIN, 'o', N'')  AS referenced_table) AS R15
                    CROSS APPLY (SELECT REPLACE(R15.referenced_table COLLATE Latin1_General_BIN, 'p', N'')  AS referenced_table) AS R16
                    CROSS APPLY (SELECT REPLACE(R16.referenced_table COLLATE Latin1_General_BIN, 'q', N'')  AS referenced_table) AS R17
                    CROSS APPLY (SELECT REPLACE(R17.referenced_table COLLATE Latin1_General_BIN, 'r', N'')  AS referenced_table) AS R18
                    CROSS APPLY (SELECT REPLACE(R18.referenced_table COLLATE Latin1_General_BIN, 's', N'')  AS referenced_table) AS R19
                    CROSS APPLY (SELECT REPLACE(R19.referenced_table COLLATE Latin1_General_BIN, 't', N'')  AS referenced_table) AS R20
                    CROSS APPLY (SELECT REPLACE(R20.referenced_table COLLATE Latin1_General_BIN, 'u', N'')  AS referenced_table) AS R21
                    CROSS APPLY (SELECT REPLACE(R21.referenced_table COLLATE Latin1_General_BIN, 'v', N'')  AS referenced_table) AS R22
                    CROSS APPLY (SELECT REPLACE(R22.referenced_table COLLATE Latin1_General_BIN, 'w', N'')  AS referenced_table) AS R23
                    CROSS APPLY (SELECT REPLACE(R23.referenced_table COLLATE Latin1_General_BIN, 'x', N'')  AS referenced_table) AS R24
                    CROSS APPLY (SELECT REPLACE(R24.referenced_table COLLATE Latin1_General_BIN, 'y', N'')  AS referenced_table) AS R25
                    CROSS APPLY (SELECT REPLACE(R25.referenced_table COLLATE Latin1_General_BIN, 'z', N'')  AS referenced_table) AS R26
                    CROSS APPLY (SELECT REPLACE(R26.referenced_table COLLATE Latin1_General_BIN, '~', N'')  AS referenced_table) AS R27
                    CROSS APPLY (SELECT REPLACE(R27.referenced_table COLLATE Latin1_General_BIN, '`', N'')  AS referenced_table) AS R28
                    CROSS APPLY (SELECT REPLACE(R28.referenced_table COLLATE Latin1_General_BIN, '!', N'')  AS referenced_table) AS R29
                    CROSS APPLY (SELECT REPLACE(R29.referenced_table COLLATE Latin1_General_BIN, '@', N'')  AS referenced_table) AS R30
                    CROSS APPLY (SELECT REPLACE(R30.referenced_table COLLATE Latin1_General_BIN, '#', N'')  AS referenced_table) AS R31
                    CROSS APPLY (SELECT REPLACE(R31.referenced_table COLLATE Latin1_General_BIN, '$', N'')  AS referenced_table) AS R32
                    CROSS APPLY (SELECT REPLACE(R32.referenced_table COLLATE Latin1_General_BIN, '%', N'')  AS referenced_table) AS R33
                    CROSS APPLY (SELECT REPLACE(R33.referenced_table COLLATE Latin1_General_BIN, '^', N'')  AS referenced_table) AS R34
                    CROSS APPLY (SELECT REPLACE(R34.referenced_table COLLATE Latin1_General_BIN, '&', N'')  AS referenced_table) AS R35
                    CROSS APPLY (SELECT REPLACE(R35.referenced_table COLLATE Latin1_General_BIN, '*', N'')  AS referenced_table) AS R36
                    CROSS APPLY (SELECT REPLACE(R36.referenced_table COLLATE Latin1_General_BIN, '(', N'')  AS referenced_table) AS R37
                    CROSS APPLY (SELECT REPLACE(R37.referenced_table COLLATE Latin1_General_BIN, ')', N'')  AS referenced_table) AS R38
                    CROSS APPLY (SELECT REPLACE(R38.referenced_table COLLATE Latin1_General_BIN, '-', N'')  AS referenced_table) AS R39
                    CROSS APPLY (SELECT REPLACE(R39.referenced_table COLLATE Latin1_General_BIN, '_', N'')  AS referenced_table) AS R40
                    CROSS APPLY (SELECT REPLACE(R40.referenced_table COLLATE Latin1_General_BIN, '=', N'')  AS referenced_table) AS R41
                    CROSS APPLY (SELECT REPLACE(R41.referenced_table COLLATE Latin1_General_BIN, '+', N'')  AS referenced_table) AS R42
                    CROSS APPLY (SELECT REPLACE(R42.referenced_table COLLATE Latin1_General_BIN, '[', N'')  AS referenced_table) AS R43
                    CROSS APPLY (SELECT REPLACE(R43.referenced_table COLLATE Latin1_General_BIN, '{', N'')  AS referenced_table) AS R44
                    CROSS APPLY (SELECT REPLACE(R44.referenced_table COLLATE Latin1_General_BIN, ']', N'')  AS referenced_table) AS R45
                    CROSS APPLY (SELECT REPLACE(R45.referenced_table COLLATE Latin1_General_BIN, '}', N'')  AS referenced_table) AS R46
                    CROSS APPLY (SELECT REPLACE(R46.referenced_table COLLATE Latin1_General_BIN, '\', N'')  AS referenced_table) AS R47
                    CROSS APPLY (SELECT REPLACE(R47.referenced_table COLLATE Latin1_General_BIN, '|', N'')  AS referenced_table) AS R48
                    CROSS APPLY (SELECT REPLACE(R48.referenced_table COLLATE Latin1_General_BIN, ':', N'')  AS referenced_table) AS R49
                    CROSS APPLY (SELECT REPLACE(R49.referenced_table COLLATE Latin1_General_BIN, ';', N'')  AS referenced_table) AS R50
                    CROSS APPLY (SELECT REPLACE(R50.referenced_table COLLATE Latin1_General_BIN, '"', N'')  AS referenced_table) AS R51
                    CROSS APPLY (SELECT REPLACE(R51.referenced_table COLLATE Latin1_General_BIN, '/', N'')  AS referenced_table) AS R52
                    CROSS APPLY (SELECT REPLACE(R52.referenced_table COLLATE Latin1_General_BIN, '?', N'')  AS referenced_table) AS R53
                    CROSS APPLY (SELECT REPLACE(R53.referenced_table COLLATE Latin1_General_BIN, '.', N'')  AS referenced_table) AS R54
                    CROSS APPLY (SELECT REPLACE(R54.referenced_table COLLATE Latin1_General_BIN, ',', N'')  AS referenced_table) AS R55
                    CROSS APPLY (SELECT REPLACE(R55.referenced_table COLLATE Latin1_General_BIN, '>', N'')  AS referenced_table) AS R56
                    CROSS APPLY (SELECT REPLACE(R56.referenced_table COLLATE Latin1_General_BIN, '<', N'')  AS referenced_table) AS R57
                    CROSS APPLY (SELECT REPLACE(R57.referenced_table COLLATE Latin1_General_BIN, ' ', N'')  AS referenced_table) AS R58
                    CROSS APPLY (SELECT REPLACE(R58.referenced_table COLLATE Latin1_General_BIN, '''', N'') AS referenced_table) AS R 
                    -- SQL Prompt formatting on
          )
        UPDATE
            TL_T
        SET
            TL_T.parent_object_id = TL_S.parent_object_id
           ,TL_T.referenced_object_id = TL_S.referenced_object_id
           ,TL_T.Depth = TL_S.Depth
           ,TL_T.referenced_schema = TL_S.referenced_schema
           ,TL_T.referenced_table = TL_S.referenced_table
           ,TL_T.referenced_alias = TL_S.referenced_alias
           ,TL_T.referenced_column = TL_S.referenced_column
           ,TL_T.parent_schema = TL_S.parent_schema
           ,TL_T.parent_table = TL_S.parent_table
           ,TL_T.parent_column = TL_S.parent_column
           ,TL_T.parent_column_is_nullable = TL_S.parent_column_is_nullable
           ,TL_T.HierarchyPath = TL_S.HierarchyPath
           ,TL_T.referenced_table_description = ISNULL(REPLACE(CAST(EP.value AS nvarchar(MAX)), N'''', N''''''), N'')
           ,TL_T.parent_alias = (
                SELECT TOP (1)
                    TL_S.referenced_alias
                FROM
                    Numbering AS TL_S
                WHERE
                    TL_T.parent_object_id = TL_S.referenced_object_id
                AND TL_T.Depth            >= TL_S.Depth
                ORDER BY
                    TL_S.referenced_alias ASC
            )
           ,TL_T.HasTriggers = CASE WHEN EXISTS (
                                             SELECT
                                                 *
                                             FROM
                                                 sys.triggers AS TG
                                             WHERE
                                                 TG.parent_id = TL_S.referenced_object_id
                                         )
                                        THEN 1
                                   ELSE 0
                               END
           ,TL_T.IsProcessed = 0
        FROM
            #TableList                          AS TL_T
        INNER JOIN Numbering                    AS TL_S ON TL_T.TableListId        = TL_S.TableListId
        LEFT OUTER JOIN sys.extended_properties AS EP ON TL_S.referenced_object_id = EP.major_id
                                                      AND EP.minor_id              = 0
                                                      AND EP.class                 = 1
                                                      AND EP.name                  = 'MS_Description';


        /**********************************************************************************************************************
        ** Set row update by varibles
        **********************************************************************************************************************/
        SELECT
            @SchemaName_Reference = referenced_schema
           ,@TableName_Reference  = referenced_table
           ,@ColumnName_Reference = referenced_column
           ,@TableAlias_Reference = referenced_alias
        FROM
            #TableList
        WHERE
            IsRowUpdateTable = 1;

        /**********************************************************************************************************************
        ** Create and load column list
        **********************************************************************************************************************/
        IF OBJECT_ID('tempdb..#ColumnList') IS NOT NULL
            BEGIN
                DROP TABLE #ColumnList;
            END;
        CREATE TABLE #ColumnList (
            ColumnListId        int           NOT NULL IDENTITY(1, 1) PRIMARY KEY
           ,schema_id           int           NOT NULL
           ,Depth               int           NOT NULL
           ,Table_object_id     int           NOT NULL
           ,SchemaName          nvarchar(MAX) NOT NULL
           ,TableName           nvarchar(MAX) NOT NULL
           ,TableAlias          nvarchar(MAX) NOT NULL
           ,column_object_id    int           NOT NULL
           ,column_id           int           NOT NULL
           ,ColumnName          nvarchar(MAX) NOT NULL
           ,ColumnNameCleaned   nvarchar(MAX) NOT NULL
           ,ColumnNameFormatted nvarchar(MAX) NOT NULL DEFAULT N''
           ,ColumnDescription   nvarchar(MAX) NOT NULL
           ,IsPrimaryKey        bit           NOT NULL
           ,IsIdentity          bit           NOT NULL
           ,IsComputed          bit           NOT NULL
           ,IsReferencedColumn  bit           NOT NULL
           ,user_type_id        int           NOT NULL
           ,TypeName            nvarchar(MAX) NOT NULL
           ,TypeLength          nvarchar(MAX) NOT NULL
           ,Is_Nullable         nvarchar(MAX) NOT NULL
           ,IsProcessed         bit           NOT NULL DEFAULT 0
           ,IsIgnore            bit           NOT NULL DEFAULT 0
           ,IsMasked            bit           NOT NULL DEFAULT 0
           ,IsRowStart          bit           NOT NULL DEFAULT 0
           ,IsRowEnd            bit           NOT NULL DEFAULT 0
        );

        /* Insert column list for tables */
        INSERT INTO #ColumnList (
            schema_id
           ,Depth
           ,Table_object_id
           ,SchemaName
           ,TableName
           ,TableAlias
           ,column_object_id
           ,column_id
           ,ColumnName
           ,ColumnNameCleaned
           ,ColumnDescription
           ,IsPrimaryKey
           ,IsIdentity
           ,IsComputed
           ,IsReferencedColumn
           ,user_type_id
           ,TypeName
           ,TypeLength
           ,Is_Nullable
           ,IsRowStart
           ,IsRowEnd
        )
        SELECT
            schema_id          = S.schema_id
           ,Depth              = TL.Depth
           ,Table_object_id    = C.object_id
           ,SchemaName         = S.name
           ,TableName          = T.name
           ,TableAlias         = TL.referenced_alias
           ,column_object_id   = C.object_id
           ,column_id          = C.column_id
           ,ColumnName         = C.name
           ,ColumnNameCleaned  = REPLACE(C.name, N' ', N'')
           ,ColumnDescription  = ISNULL(REPLACE(CAST(EP.value AS nvarchar(MAX)), N'''', N''''''), N'')
           ,IsPrimaryKey       = CASE WHEN PK.object_id IS NOT NULL THEN 1 ELSE 0 END
           ,IsIdentity         = C.is_identity
           ,IsComputed         = C.is_computed
           ,IsReferencedColumn = CASE WHEN EXISTS (
                                               SELECT
                                                   *
                                               FROM
                                                   #TableList AS TLSub
                                               WHERE
                                                   TLSub.referenced_schema = S.name COLLATE Latin1_General_100_CI_AS
                                               AND TLSub.referenced_table  = T.name COLLATE Latin1_General_100_CI_AS
                                               AND TLSub.parent_column     = C.name COLLATE Latin1_General_100_CI_AS
                                           )
                                          THEN 1
                                     ELSE 0
                                 END
           ,user_type_id       = C.user_type_id
           ,TypeName           = CASE WHEN TP.name = 'timestamp' THEN 'rowversion' ELSE TP.name END
           ,TypeLength         =
           /* decimal, numeric */
           CASE WHEN C.user_type_id IN (106, 108)
                    THEN CAST(N'(' AS nvarchar(MAX)) + CAST(C.precision AS varchar(3)) + CAST(N', ' AS nvarchar(MAX))
                         + CAST(C.scale AS varchar(3)) + CAST(N')' AS nvarchar(MAX))
               ELSE CAST(N'' AS nvarchar(MAX))
           END +
           /* datetime2, datetimeoffset, time */
           CASE WHEN C.user_type_id IN (41, 42, 43)
                    THEN CAST(N'(' AS nvarchar(MAX)) + CAST(C.scale AS varchar(3)) + CAST(N')' AS nvarchar(MAX))
               ELSE CAST(N'' AS nvarchar(MAX))
           END +
           /* varbinary, binary, varchar, char */
           CASE WHEN C.user_type_id IN (165, 167, 173, 175)
                    THEN CAST(N'(' AS nvarchar(MAX)) + CASE WHEN C.max_length = -1
                                                                THEN CAST(N'MAX' AS nvarchar(MAX))
                                                           ELSE CAST(C.max_length AS varchar(4))
                                                       END + CAST(N')' AS nvarchar(MAX))
               ELSE CAST(N'' AS nvarchar(MAX))
           END +
           /* nvarchar, nchar */
           CASE WHEN C.user_type_id IN (231, 239)
                    THEN CAST(N'(' AS nvarchar(MAX)) + CASE WHEN C.max_length = -1
                                                                THEN CAST(N'MAX' AS nvarchar(MAX))
                                                           ELSE CAST(C.max_length / 2 AS varchar(4))
                                                       END + CAST(N')' AS nvarchar(MAX))
               ELSE CAST(N'' AS nvarchar(MAX))
           END
           ,Is_Nullable        = CAST(CASE WHEN C.is_nullable = 1
                                               THEN CAST(N'NULL' AS nvarchar(MAX))
                                          ELSE CAST(N'NOT NULL' AS nvarchar(MAX))
                                      END AS nvarchar(MAX))
           ,IsRowStart         = IIF(C.generated_always_type = 1, 1, 0)
           ,IsRowEnd           = IIF(C.generated_always_type = 2, 1, 0)
        FROM
            sys.columns                         AS C
        INNER JOIN sys.tables                   AS T ON C.object_id      = T.object_id
        INNER JOIN sys.schemas                  AS S ON T.schema_id      = S.schema_id
        INNER JOIN sys.objects                  AS SO ON SO.object_id    = C.object_id
        INNER JOIN sys.types                    AS TP ON TP.user_type_id = C.user_type_id
        INNER JOIN #TableList                   AS TL ON C.object_id     = TL.referenced_object_id
        LEFT OUTER JOIN sys.extended_properties AS EP ON C.object_id     = EP.major_id
                                                      AND C.column_id    = EP.minor_id
                                                      AND EP.class       = 1
                                                      AND EP.name        = 'MS_Description'
        LEFT OUTER JOIN (
            SELECT
                C.object_id
               ,C.column_id
            FROM
                sys.indexes              AS I
            INNER JOIN sys.index_columns AS IC ON I.object_id = IC.object_id
                                               AND I.index_id = IC.index_id
            INNER JOIN sys.columns       AS C ON IC.object_id = C.object_id
                                              AND C.column_id = IC.column_id
            WHERE
                I.is_primary_key = 1
        )                                       AS PK ON C.object_id     = PK.object_id
                                                      AND C.column_id    = PK.column_id
        WHERE
            SO.type = 'U'
        ORDER BY
            TL.TableListId ASC
           ,C.column_id ASC
        OPTION (RECOMPILE);



        /**********************************************************************************************************************
        ** Process formatted column name
        **********************************************************************************************************************/
        WHILE EXISTS (SELECT * FROM #ColumnList WHERE IsProcessed = 0)
            BEGIN
                SELECT TOP (1)
                    @ColumnListId  = CL.ColumnListId
                   ,@ColumnNameRaw = REPLACE(CL.ColumnName, N' ', N'')
                FROM
                    #ColumnList AS CL
                WHERE
                    CL.IsProcessed = 0
                ORDER BY
                    CL.ColumnListId ASC;

                SELECT
                    @i                   = 1
                   ,@ColumnNameRawLength = LEN(@ColumnNameRaw)
                   ,@ColumnNameFormatted = N'';

                WHILE @i <= @ColumnNameRawLength
                    BEGIN
                        SELECT
                            @PreviousCharacter = SUBSTRING(@ColumnNameRaw, @i - 1, 1)
                           ,@CurrentCharacter  = SUBSTRING(@ColumnNameRaw, @i + 0, 1)
                           ,@NextCharacter     = SUBSTRING(@ColumnNameRaw, @i + 1, 1);

                        IF @CurrentCharacter = UPPER(@CurrentCharacter)COLLATE Latin1_General_CS_AS
                            BEGIN
                                IF @CurrentCharacter = UPPER(@CurrentCharacter)COLLATE Latin1_General_CS_AS
                                AND (
                                    @PreviousCharacter <> UPPER(@PreviousCharacter)COLLATE Latin1_General_CS_AS
                                    OR @NextCharacter <> UPPER(@NextCharacter)COLLATE Latin1_General_CS_AS
                                    OR @FormatColumnNamesPreserveAdjacentCaps = 0
                                )
                                AND @PreviousCharacter <> N' '
                                AND @CurrentCharacter <> N' '
                                    BEGIN
                                        SET @ColumnNameFormatted = @ColumnNameFormatted + N' ';
                                    END;
                            END;

                        SET @ColumnNameFormatted = @ColumnNameFormatted + @CurrentCharacter;

                        SET @i = @i + 1;
                    END;

                UPDATE
                    #ColumnList
                SET
                    IsProcessed = 1
                   ,ColumnNameFormatted = IIF(@FormatColumnNames = 1, @ColumnNameFormatted, @ColumnNameRaw)
                WHERE
                    ColumnListId = @ColumnListId;
            END;


        /**********************************************************************************************************************
        ** Update ignored columns
        **********************************************************************************************************************/
        UPDATE
            CL
        SET
            CL.IsIgnore = 1
        FROM
            #ColumnList AS CL
        WHERE
            EXISTS (
            SELECT * FROM #IgnoreColumns AS IC WHERE CL.ColumnName = IC.ColumnName
        )
        AND CL.IsPrimaryKey = 0;


        /**********************************************************************************************************************
        ** Update masked columns
        **********************************************************************************************************************/
        UPDATE
            CL
        SET
            CL.IsMasked = 1
        FROM
            #ColumnList AS CL
        WHERE
            EXISTS (
            SELECT * FROM #MaskColumns AS IC WHERE CL.ColumnName = IC.ColumnName
        )
        AND CL.IsPrimaryKey = 0;

        /* Build the list for the IN() command */
        SELECT
            @MaskList = @MaskList + N'N'''
                        + CASE WHEN @FormatColumnNames = 0 THEN CL.ColumnName ELSE CL.ColumnNameFormatted END + N''', '
        FROM
            #ColumnList AS CL
        WHERE
            CL.IsMasked = 1;

        /* Fix the last comma */
        IF LEN(@MaskList) > 0
            BEGIN
                SET @MaskList = LEFT(@MaskList, LEN(@MaskList) - 1);
            END;


        /**********************************************************************************************************************
        ** Set primary key variable
        **********************************************************************************************************************/
        SELECT TOP (1)
            @PrimaryKeyTableColumn = QUOTENAME(TableAlias) + N'.' + QUOTENAME(ColumnName)
        FROM
            #ColumnList
        WHERE
            IsPrimaryKey = 1
        ORDER BY
            ColumnListId ASC;


        /**********************************************************************************************************************
        ** Set valid from time variable
        **********************************************************************************************************************/
        SELECT TOP (1)
            @ValidFromTimeTableColumn = QUOTENAME(TableAlias) + N'.' + QUOTENAME(ColumnName)
        FROM
            #ColumnList
        WHERE
            IsRowStart = 1
        ORDER BY
            ColumnListId ASC;


        /**********************************************************************************************************************
        ** Set table alias variable
        **********************************************************************************************************************/
        SELECT TOP (1)
            @TableAlias = referenced_alias
        FROM
            #TableList
        WHERE
            Depth = 0
        ORDER BY
            TableListId ASC;


        /**********************************************************************************************************************
        ** Build the column list
        **********************************************************************************************************************/
        SELECT
            @TemporalWithQueryColumns = @TemporalWithQueryColumns + @NewLineString + N'            ,'
                                        + CASE WHEN IsPrimaryKey = 1
                                               OR IsRowStart = 1
                                                   THEN IIF(IsRowStart = 1
                                                        ,'[' + @ChangedTimeResultColumnName + N']'
                                                        ,'[' + @PrimaryKeyResultColumnName + N']') + N' = '
                                                        + QUOTENAME(TableAlias) + N'.' + QUOTENAME(ColumnName)
                                              ELSE
                                                  QUOTENAME('New' + ColumnName) + N' = ' + QUOTENAME(TableAlias) + N'.'
                                                  + QUOTENAME(ColumnName) + @NewLineString + N'            ,'
                                                  + QUOTENAME('Old' + ColumnName) + N' = ' + 'LAG('
                                                  + QUOTENAME(TableAlias) + N'.' + QUOTENAME(ColumnName)
                                                  + N', 1, NULL) OVER (PARTITION BY ' + @PrimaryKeyTableColumn
                                                  + N' ORDER BY ' + @ValidFromTimeTableColumn + N' ASC)'
                                          END
        FROM
            #ColumnList
        WHERE
            (IsIgnore = 0 OR IsRowStart = 1)
        AND IsRowEnd  <> 1
        AND TypeName NOT IN ('Image', 'RowVersion');

        /* Fix the first item */
        IF LEN(@TemporalWithQueryColumns) > 0
            BEGIN
                SET @TemporalWithQueryColumns = RIGHT(@TemporalWithQueryColumns, LEN(@TemporalWithQueryColumns) - 15);
            END;


        /**********************************************************************************************************************
        ** Build the temporal WITH query WHERE clause
        **********************************************************************************************************************/
        IF @PrimaryKeyValue IS NOT NULL
        OR @PrimaryKeyValue <> N''
            BEGIN
                SET @TemporalWithQueryWhere = N'
        WHERE
            ' + @PrimaryKeyTableColumn + N' = ' + @PrimaryKeyValue;
            END;


        /**********************************************************************************************************************
        ** Build the change by FROM text 
        **********************************************************************************************************************/
        IF @RowUpdatePersonColumnName IS NOT NULL
            BEGIN
                SET @ChangedByFrom = N'
        LEFT OUTER JOIN ' + QUOTENAME(@SchemaName_Reference) + N'.' + QUOTENAME(@TableName_Reference) + N' AS '
                                     + QUOTENAME(@TableAlias_Reference) + N' ON ' + QUOTENAME(@TableAlias) + N'.'
                                     + QUOTENAME(@RowUpdatePersonColumnName) + N' = '
                                     + QUOTENAME(@TableAlias_Reference) + N'.' + QUOTENAME(@ColumnName_Reference);
            END;


        /**********************************************************************************************************************
        ** Build the CROSS APPLY casts
        **********************************************************************************************************************/
        SELECT
            @TemporalCrossApplyColumns = @TemporalCrossApplyColumns + @NewLineString + N'        ,('''
                                         + ColumnNameFormatted + N''', CAST([T].' + QUOTENAME(N'New' + ColumnName)
                                         + N' AS nvarchar(max)), CAST([T].' + QUOTENAME(N'Old' + ColumnName)
                                         + N' AS nvarchar(max)))'
        FROM
            #ColumnList
        WHERE
            IsIgnore     = 0
        AND IsRowStart   = 0
        AND IsRowEnd     <> 1
        AND TypeName NOT IN ('Image', 'RowVersion')
        AND IsPrimaryKey = 0;

        /* Fix the first item */
        IF LEN(@TemporalCrossApplyColumns) > 0
            BEGIN
                SET @TemporalCrossApplyColumns = RIGHT(@TemporalCrossApplyColumns, LEN(@TemporalCrossApplyColumns) - 11);
            END;


        /**********************************************************************************************************************
        ** Build the query
        **********************************************************************************************************************/
        SET @ExecuteChangesGetString = N'
/* Executed by stored procedure named [dbo].[TemporalTableChangesGet] */
;WITH Temporal
  AS (
        SELECT
             ' + @TemporalWithQueryColumns;

        IF @RowUpdatePersonColumnNameValue IS NOT NULL
            BEGIN
                SET @ExecuteChangesGetString = @ExecuteChangesGetString + N'
            ,['                                + @ChangedByResultColumnName + N'] = '
                                               + QUOTENAME(ISNULL(@TableAlias_Reference, @TableAlias)) + N'.'
                                               + QUOTENAME(@RowUpdatePersonColumnNameValue) + N'';
            END;

        SET @ExecuteChangesGetString = @ExecuteChangesGetString + N'
        FROM
            '                          + QUOTENAME(@SchemaName) + CAST(N'.' AS nvarchar(MAX)) + QUOTENAME(@TableName)
                                       + N' FOR SYSTEM_TIME ALL AS ' + QUOTENAME(@TableAlias) + @ChangedByFrom
                                       + @TemporalWithQueryWhere + N'
  )
SELECT
     ['                                + @PrimaryKeyResultColumnName + N'] = [T].[' + @PrimaryKeyResultColumnName
                                       + N']
    ,['                                + @ColumnNameResultColumnName + N'] = [CA].[' + @ColumnNameResultColumnName
                                       + N']';
        IF LEN(@MaskList) > 0
            BEGIN
                SET @ExecuteChangesGetString = @ExecuteChangesGetString + N'
    ,['                                        + @OldValueResultColumnName + N'] = CASE WHEN [CA].['
                                               + @ColumnNameResultColumnName + N'] IN (' + @MaskList
                                               + N') THEN ''****'' ELSE CA.[' + @OldValueResultColumnName
                                               + N'] END
    ,['                                        + @NewValueResultColumnName + N'] = CASE WHEN [CA].['
                                               + @ColumnNameResultColumnName + N'] IN (' + @MaskList
                                               + N') THEN ''****'' ELSE CA.[' + @NewValueResultColumnName
                                               + N'] END
    '           ;
            END;
        ELSE
            BEGIN
                SET @ExecuteChangesGetString = @ExecuteChangesGetString + N'
    ,['                                        + @OldValueResultColumnName + N'] = ISNULL(CA.['
                                               + @OldValueResultColumnName + N'], N'''')
    ,['                                        + @NewValueResultColumnName + N'] = ISNULL(CA.['
                                               + @NewValueResultColumnName + N'], N'''')
    '           ;
            END;

        IF @RowUpdatePersonColumnNameValue IS NOT NULL
            BEGIN
                SET @ExecuteChangesGetString = @ExecuteChangesGetString + N',[' + @ChangedByResultColumnName
                                               + N'] = ISNULL([T].[' + @ChangedByResultColumnName
                                               + N'], N''[UNKNOWN]'')
    '           ;
            END;

        SET @ExecuteChangesGetString = @ExecuteChangesGetString + N',[' + @ChangedTimeResultColumnName + N'] = [T].['
                                       + @ChangedTimeResultColumnName
                                       + N']
FROM
    [Temporal] AS [T]
CROSS APPLY (
    VALUES 
         '                             + @TemporalCrossApplyColumns + N'
) AS [CA] (['                          + @ColumnNameResultColumnName + N'], [' + @NewValueResultColumnName + N'], ['
                                       + @OldValueResultColumnName + N'])
WHERE
    EXISTS (SELECT [CA].['             + @NewValueResultColumnName + N'] EXCEPT SELECT [CA].['
                                       + @OldValueResultColumnName + N'])
ORDER BY
    [T].['                             + @ChangedTimeResultColumnName + N'] ' + @OrderBy + N';';

        IF @Debug = 1
            BEGIN
                SELECT
                    [processing-instruction(output)] = CAST(N'/* Click here to view the generated code.
Copy just the T-SQL below this block comment into a new query window to execute. */

' AS nvarchar(MAX)  )                                  + @ExecuteChangesGetString
                                                       + N'


/* Copy just the T-SQL above this block comment into a new query window to execute. */
'
                FOR XML PATH('');
            END;
        ELSE
            BEGIN
                EXEC sys.sp_executesql @stmt = @ExecuteChangesGetString;
            END;
    END;

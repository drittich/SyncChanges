﻿using Dapper;

using Humanizer;

using NLog;

using NPoco;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace SyncChanges
{
	/// <summary>
	/// Allows replication of database changes from a source database to one or more destination databases.
	/// </summary>
	public class Synchronizer
	{
		/// <summary>
		/// Gets or sets a value indicating whether destination databases will be modified during a replication run.
		/// </summary>
		/// <value>
		///   <c>true</c> if destination databases will be modified; otherwise, <c>false</c>.
		/// </value>
		public bool DryRun { get; set; } = false;

		/// <summary>
		/// Gets or sets the database connection timeout.
		/// </summary>
		/// <value>
		/// The database connection timeout.
		/// </value>
		public int Timeout { get; set; } = 0;

		/// <summary>
		/// Gets or sets the minimum synchronization time interval in seconds. Default is 30 seconds.
		/// </summary>
		/// <value>
		/// The synchronization time interval in seconds.
		/// </value>
		public int Interval { get; set; } = 30;

		/// <summary>
		/// Occurs when synchronization from a synchronization loop has succeeded.
		/// </summary>
		public event EventHandler<SyncEventArgs> Synced;

		static Logger Log = LogManager.GetCurrentClassLogger();
		Config Config { get; set; }
		bool Error { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="Synchronizer"/> class.
		/// </summary>
		/// <param name="config">The configuration.</param>
		/// <exception cref="System.ArgumentException"><paramref name="config"/> is null</exception>
		public Synchronizer(Config config)
		{
			Config = config ?? throw new ArgumentException("config is null", nameof(config));
		}

		private IList<IList<TableInfo>> Tables { get; } = new List<IList<TableInfo>>();
		private bool Initialized = false;

		/// <summary>
		/// Initialize the synchronization process.
		/// </summary>
		public void Init()
		{
			if (Timeout != 0)
				Log.Info($"Command timeout is {"second".ToQuantity(Timeout)}");

			for (int i = 0; i < Config.ReplicationSets.Count; i++)
			{
				var replicationSet = Config.ReplicationSets[i];

				Log.Info($"Getting replication information for replication set {replicationSet.Name}");

				var tables = GetTables(replicationSet.Source);
				if (replicationSet.Tables != null && replicationSet.Tables.Any())
					tables = tables
						.Select(t => new { Table = t, Name = t.Name.Replace("[", "").Replace("]", "") })
						.Where(t => replicationSet.Tables.Any(r => r == t.Name || r == t.Name.Split('.')[1]))
						.Select(t => t.Table).ToList();

				if (!tables.Any())
					Log.Warn("No tables to replicate (check if change tracking is enabled)");
				else
					Log.Info($"Replicating {"table".ToQuantity(tables.Count, ShowQuantityAs.None)} \n\t{string.Join("\n\t", tables.Select(t => t.Name))}");

				Tables.Add(tables);
			}

			Initialized = true;
		}

		/// <summary>
		/// Perform the synchronization.
		/// </summary>
		/// <returns>true, if the synchronization was successful; otherwise, false.</returns>
		public bool Sync()
		{
			Error = false;

			if (!Initialized) Init();

			for (int i = 0; i < Config.ReplicationSets.Count; i++)
			{
				var replicationSet = Config.ReplicationSets[i];
				var tables = Tables[i];

				Sync(replicationSet, tables);
			}

			Log.Info($"Finished replication {(Error ? "with" : "without")} errors");

			return !Error;
		}

		private bool Sync(ReplicationSet replicationSet, IList<TableInfo> tables, long sourceVersion = -1)
		{
			Error = false;

			if (!tables.Any()) return true;

			Log.Info($"Starting replication for replication set {replicationSet.Name}");

			var destinationsByVersion = replicationSet.Destinations.GroupBy(d => GetCurrentVersion(d))
				.Where(d => d.Key >= 0 && (sourceVersion < 0 || d.Key < sourceVersion)).ToList();

			foreach (var destinations in destinationsByVersion)
				Replicate(replicationSet.Source, destinations, tables);

			return !Error;
		}

		public void EnableChangeTrackingInDb(ReplicationSet replicationSet, bool enable = true)
		{
			var dbName = Sql.GetDbNameFromConnectionString(replicationSet.Source.ConnectionString);
			var sql = $@"alter database [{dbName}] set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)";
			using (var cn = Sql.GetConnection(replicationSet.Source.ConnectionString))
				cn.Execute(sql);
		}

		/// <summary>
		/// Turn on SQL Change Tracking for the given tables.
		/// Assumes we do not want Column Tracking enabled.
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="tables"></param>
		public void EnableChangeTrackingForTables(string connectionString, IEnumerable<string> tables)
		{
			using (var cn = Sql.GetConnection(connectionString))
			{
				foreach (var table in tables)
				{
					var sql = $@"
						alter table {Sql.NormalizeObjectName(table, null)}
						enable CHANGE_TRACKING
						with (TRACK_COLUMNS_UPDATED = OFF)";

					cn.Execute(sql);
				}
			}
		}

		/// <summary>
		/// Returns the specified tables and views from the config, along with all of the 
		/// additional tables and function dependencies of the views
		/// functions or other tables/views
		/// </summary>
		/// <param name="replicationSet"></param>
		/// <returns></returns>
		public List<SyncObject> GetSyncObjectsWithDependencies(ReplicationSet replicationSet)
		{
			var ret = new List<SyncObject>();

			// figure out all the views we need
			var requestedViews = replicationSet.Views;
			var dependencyViews = GetDependencyViews(replicationSet.Source.ConnectionString, requestedViews);
			var allViews = requestedViews.Concat(dependencyViews).Distinct().ToList();
			foreach (var v in allViews)
				ret.Add(new SyncObject() { Name = v, Type = SyncObject.ObjectType.View });

			// now figure out all the tables we need
			var requestedTables = replicationSet.Tables;
			var dependencyTables = GetDependencyTablesForViews(replicationSet.Source.ConnectionString, allViews);
			var allTables = requestedTables.Concat(dependencyTables).Distinct().ToList();
			foreach (var t in allTables)
				ret.Add(new SyncObject() { Name = t, Type = SyncObject.ObjectType.Table });

			// TODO: figure out what functions we need to sync for views

			return ret;
		}

		/// <summary>
		/// Making use of SQL Change Tracking, does the initial data
		/// population to bring the source and destination tables in sync
		/// </summary>
		/// <param name="sourceConnectionString"></param>
		/// <param name="destinationConnectionString"></param>
		/// <param name="table"></param>
		public void DoInitialDataPopulationForTable(string sourceConnectionString, string destinationConnectionString, string table)
		{
			// get current sync version
			// TODO: how do we save this with SyncChanges on initial load?
			// Obtain the current synchronization version. This will be used next time that changes are obtained.
			long? curVersion = GetChangeTrackingCurrentVersion(sourceConnectionString);
			BulkCopy(sourceConnectionString, destinationConnectionString, curVersion, table, null, true);
		}

		private static void BulkCopy(string sourceConnectionString, string destinationConnectionString, long? curVersion, string sourceTable, string destinationTable = null, bool truncateFirst = false)
		{
			if (destinationTable == null)
				destinationTable = sourceTable;

			destinationTable = Sql.NormalizeObjectName(destinationTable, null);

			Log.Info($"Bulk-copying table {sourceTable}");

			var sw = Stopwatch.StartNew();
			using (var cnSource = Sql.GetConnection(sourceConnectionString))
			{
				// TODO: modify this select to exclude INSERTs from versions higher than curVersion
				var sqlSelectAll = $@"select * from {Sql.NormalizeObjectName(sourceTable, null)}";
				var cmdSelectAll = new SqlCommand(sqlSelectAll, cnSource);

				using (var rdr = cmdSelectAll.ExecuteReader())
				{
					using (var cnDestination = Sql.GetConnection(destinationConnectionString))
					{
						if (truncateFirst)
						{
							var cmdTruncateDestination = new SqlCommand($"truncate table {destinationTable}", cnDestination);
							cmdTruncateDestination.ExecuteNonQuery();
							Log.Info("Table truncated");
						}
						else
						{
							Log.Info("Truncate skipped");
						}
						// Note, may not be able to use TableLock with Azure, see https://www.adathedev.co.uk/2011/01/sqlbulkcopy-to-sql-server-in-parallel.html
						using (var bc = new SqlBulkCopy(cnDestination, SqlBulkCopyOptions.TableLock, null))
						{
							bc.BatchSize = 4000; // see https://www.adathedev.co.uk/2011/01/sqlbulkcopy-to-sql-server-in-parallel.html, 4000 may be maximum for Azure SQL
							bc.NotifyAfter = 1000;
							bc.SqlRowsCopied += (sender, eventArgs) =>
							{
								//Log.Info($"{eventArgs.RowsCopied} loaded");
							};

							bc.DestinationTableName = destinationTable;
							// Below not needed when table structures are the same
							//bc.ColumnMappings.Add("NotificationGroupEventTypeID", "NotificationGroupEventTypeID");
							//bc.ColumnMappings.Add("NotificationGroupID", "NotificationGroupID");
							//bc.ColumnMappings.Add("EventTypeID", "EventTypeID");
							// ...

							try
							{
								bc.WriteToServer(rdr);
							}
							catch (Exception ex)
							{
								Log.Error(ex.Message);
							}
							finally
							{
								// Close the SqlDataReader. The SqlBulkCopy
								// object is automatically closed at the end
								// of the using block.
								rdr.Close();
							}
						}

						var cmdRowCount = new SqlCommand($"select count(*) from {destinationTable}", cnDestination);
						long count = Convert.ToInt32(cmdRowCount.ExecuteScalar());
						Log.Info($"{count} rows loaded");
					}
				}
			}
			Log.Info($"Took {sw.ElapsedMilliseconds} ms\n");
		}


		/// <summary>
		/// Given a list of tables, returns the ones that do not exist in the destination database.
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="syncTables"></param>
		/// <returns></returns>
		public List<string> GetNonExistingSyncTables(string connectionString, IEnumerable<string> syncTables)
		{
			var sql = @"
				select null
				from INFORMATION_SCHEMA.TABLES
				where (PARSENAME(@table, 2) is null and TABLE_SCHEMA = 'dbo'
						or PARSENAME(@table, 2) = TABLE_SCHEMA)
					and PARSENAME(@table, 1) = TABLE_NAME";

			var ret = new List<string>();

			using (var cn = Sql.GetConnection(connectionString))
			{
				foreach (var table in syncTables)
				{
					if (!cn.Query(sql, new { table }).Any())
						ret.Add(table);
				}
			}

			return ret;
		}

		/// <summary>
		/// Given a list of views, returns a distinct list of views that
		/// those views depend on.
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="views"></param>
		/// <returns></returns>
		private List<string> GetDependencyViews(string connectionString, List<string> views)
		{
			var ret = new List<string>();

			var sql = @"
				;with DepTree (referenced_id, referenced_name, SchemaName)
				 AS 
				(
					select o.[object_id] AS referenced_id , 
						o.name AS referenced_name,
						s.name  as SchemaName
					from sys.objects o 
					inner join sys.views v on o.object_id = v.object_id
					inner join sys.schemas s on v.schema_id = s.schema_id
					where o.name = PARSENAME(@view, 1)
					and s.name = isnull(PARSENAME(@view, 2), 'dbo')
    
					union all
    
					select d1.referenced_id,  
						OBJECT_NAME( d1.referenced_id),
						s.name  as SchemaName
					from sys.sql_expression_dependencies d1 
					inner join DepTree r ON r.referenced_id = d1.referencing_id
					inner join sys.views v on d1.referenced_id = v.object_id
					inner join sys.schemas s on v.schema_id = s.schema_id
				)
				select quotename(SchemaName) + '.' + quotename(referenced_name) as SchemaAndViewName
				FROM DepTree 
			";

			using (var cn = Sql.GetConnection(connectionString))
			{
				foreach (var view in views)
				{
					var result = cn.Query<string>(sql, new { view });
					ret.AddRange(result);
				}
			}

			return ret.Distinct().ToList();
		}

		/// <summary>
		/// List the tables with no PK
		/// </summary>
		/// <returns></returns>
		public List<string> GetTablesToSyncWithNoPk(List<string> tablesToSync)
		{
			/*
				SELECT TABLE_SCHEMA,
				  TABLE_NAME
				FROM INFORMATION_SCHEMA.TABLES
				WHERE OBJECTPROPERTY(OBJECT_ID(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)),'TableHasPrimaryKey') = 0 
					AND TABLE_TYPE='BASE TABLE'
				ORDER BY TABLE_SCHEMA, TABLE_NAME
			*/
			throw new NotImplementedException();
		}

		/// <summary>
		/// Given a list of views, this function returns a unique list of
		/// all the tables those views depend on. 
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="views">Expects schema and object name, e.g., dbo.vUser</param>
		/// <returns></returns>
		private List<string> GetDependencyTablesForViews(string connectionString, List<string> views)
		{
			var ret = new List<string>();

			var sql = @"
				select distinct quotename(schema_name(o.schema_id)) + '.' + quotename(o.name)
					-- schema_name(v.schema_id) as schema_name,
					-- v.name as view_name,
					-- schema_name(o.schema_id) as referenced_schema_name,
					-- o.name as referenced_entity_name,
					-- o.type_desc as entity_type
				from sys.views v
				inner join sys.sql_expression_dependencies d on d.referencing_id = v.object_id
					and d.referenced_id is not null
				inner join sys.objects o on o.object_id = d.referenced_id
				where o.type_desc = 'USER_TABLE'
					and schema_name(v.schema_id) = isnull(PARSENAME(@view, 2), 'dbo')
					and v.name = PARSENAME(@view, 1) 
				order by 1";

			using (var cn = Sql.GetConnection(connectionString))
			{
				foreach (var view in views)
				{
					var result = cn.Query<string>(sql, new { view });
					ret.Concat(result);
				}
			}

			return ret.Distinct().ToList();
		}

		/// <summary>
		/// This creates tables in the destination that do not exist
		/// </summary>
		/// <param name="sourceConnectionString"></param>
		/// <param name="destinationConnectiontring"></param>
		/// <param name="table"></param>
		/// <param name="destinationSchema"></param>
		public void CreateDestinationTable(string sourceConnectionString, string destinationConnectiontring, string table, string destinationSchema = null)
		{
			string createStatement = GetCreateTableStatement(sourceConnectionString, table, destinationSchema);

			using (var cn = Sql.GetConnection(destinationConnectiontring))
				cn.Execute(createStatement);
		}

		/// <summary>
		/// Gets the SQL statement to generate the table to receive imported table.
		/// This table will have no PK, triggers, indexes, calculated columns,
		/// foreign key references, etc.
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="table"></param>
		/// <param name="schema"></param>
		/// <returns></returns>
		public static string GetCreateTableStatement(string connectionString, string table, string schema = null)
		{
			string createStatementSql;

			using (var cn = Sql.GetConnection(connectionString))
				createStatementSql = cn.Query<string>(GetGenerateCreateTableStatement(), new { table, destinationSchema = schema }).Single();

			return createStatementSql;
		}

		/// <summary>
		/// This gets the SQL statement that will iteself generate a CREATE TABLE sql statement.
		/// </summary>
		/// <returns></returns>
		public static string GetGenerateCreateTableStatement()
		{
			var createSelectStatementSql = @"
				--declare @table sysname = 'dbo.[TypeTest]'
				--declare @destinationSchema sysname = null

				select 'create table [' + isnull(@destinationSchema, t.TABLE_SCHEMA) + '].[' + t.TABLE_NAME + '] (' + char(10)
					+ o.columnList 
					+ ')'
				from INFORMATION_SCHEMA.TABLES t
				cross apply (
					SELECT
						char(9) + 
						'[' + c.column_name + '] ' + 
						c.data_type + 
						case c.data_type
							when 'geography' then ''
							when 'geometry' then ''
							when 'hierarchyid' then ''
							when 'ntext' then ''
							when 'sql_variant' then ''
							when 'text' then ''
							when 'xml' then ''
							when 'datetime2' then '(' + cast(c.DATETIME_PRECISION as varchar) + ')'
							when 'datetimeoffset' then '(' + cast(c.DATETIME_PRECISION as varchar) + ')'
							when 'decimal' then '(' + cast(c.numeric_precision as varchar) + ', ' + cast(c.numeric_scale as varchar) + ')'
							when 'numeric' then '(' + cast(c.numeric_precision as varchar) + ', ' + cast(c.numeric_scale as varchar) + ')'
							when 'time' then '(' + cast(c.DATETIME_PRECISION as varchar) + ')'
							else coalesce('(' + case when c.character_maximum_length = -1 then 'MAX' else cast(c.character_maximum_length as varchar) end +')','') 
						end +
						case when IS_NULLABLE = 'NO' then ' not' else '' end + ' null' + 
						',' + char(10)
					from information_schema.columns c
					where c.TABLE_SCHEMA = t.TABLE_SCHEMA
						and c.TABLE_NAME = t.TABLE_NAME
					order by c.ordinal_position
					FOR XML PATH('')
				) o (columnList)
				where t.TABLE_SCHEMA = isnull(PARSENAME(@table, 2), 'dbo')
					and t.TABLE_NAME = PARSENAME(@table, 1)      
				";

			return createSelectStatementSql;
		}

		/// <summary>
		/// This compares CREATE TABLE statements from both source and destination and makes sure they match exactly
		/// </summary>
		/// <param name="sourceConnectionString"></param>
		/// <param name="destinationConnectiontring"></param>
		/// <param name="table"></param>
		/// <param name="destinationSchema"></param>
		public static bool IsTableSchemaInSync(string sourceConnectionString, string destinationConnectiontring, string table, string destinationSchema = null)
		{
			var sourceCreateSql = GetCreateTableStatement(sourceConnectionString, table);
			var destinationCreateSql = GetCreateTableStatement(destinationConnectiontring, table, destinationSchema);

			return sourceCreateSql.Equals(destinationCreateSql, StringComparison.OrdinalIgnoreCase);
		}

		/// <summary>
		/// Performs synchronization in an infinite loop. Periodically checks if source version has increased to trigger replication.
		/// </summary>
		/// <param name="token">The cancellation token.</param>
		public void SyncLoop(CancellationToken token)
		{
			var currentVersions = Enumerable.Repeat(0L, Config.ReplicationSets.Count).ToList();

			if (!Initialized) Init();

			while (true)
			{
				if (token.IsCancellationRequested)
				{
					Log.Info("Stopping replication.");
					return;
				}

				var start = DateTime.UtcNow;
				Error = false;

				for (int i = 0; i < Config.ReplicationSets.Count; i++)
				{
					var replicationSet = Config.ReplicationSets[i];
					var currentVersion = currentVersions[i];
					long version = 0;

					try
					{
						using (var db = GetDatabase(replicationSet.Source.ConnectionString, DatabaseType.SqlServer2008))
							version = GetChangeTrackingCurrentVersion(db).Value;

						Log.Debug($"Current version of source in replication set {replicationSet.Name} is {version}.");

						if (version > currentVersion)
						{
							Log.Info($"Current version of source in replication set {replicationSet.Name} has increased from {currentVersion} to {version}: Starting replication.");

							var tables = Tables[i];
							var success = Sync(replicationSet, tables, version);

							if (success) currentVersions[i] = version;

							Synced?.Invoke(this, new SyncEventArgs { ReplicationSet = replicationSet, Version = version });
						}
					}
					catch (Exception ex)
					{
						Log.Error(ex, $"Error occurred during replication of set {replicationSet.Name}.");
						Error = true;
					}

					if (token.IsCancellationRequested)
					{
						Log.Info("Stopping replication.");
						return;
					}
				}

				Log.Info($"Finished replication {(Error ? "with" : "without")} errors");

				var delay = (int)Math.Round(Math.Max(0, (TimeSpan.FromSeconds(Interval) - (DateTime.UtcNow - start)).TotalSeconds) * 1000, MidpointRounding.AwayFromZero);
				Thread.Sleep(delay);
			}
		}

		private IList<TableInfo> GetTables(DatabaseInfo dbInfo)
		{
			try
			{
				using (var db = GetDatabase(dbInfo.ConnectionString, DatabaseType.SqlServer2008))
				{
					var sql = @"select TableName, ColumnName, iif(max(cast(is_primary_key as tinyint)) = 1, 1, 0) PrimaryKey from
                        (
                        select ('[' + s.name + '].[' + t.name + ']') TableName, ('[' + COL_NAME(t.object_id, a.column_id) + ']') ColumnName,
                        i.is_primary_key
                        from sys.change_tracking_tables tr
                        join sys.tables t on t.object_id = tr.object_id
                        join sys.schemas s on s.schema_id = t.schema_id
                        join sys.columns a on a.object_id = t.object_id
                        left join sys.index_columns c on c.object_id = t.object_id and c.column_id = a.column_id
                        left join sys.indexes i on i.object_id = t.object_id and i.index_id = c.index_id
                        where a.is_computed = 0
                        ) X
                        group by TableName, ColumnName
                        order by TableName, ColumnName";

					var tables = db.Fetch<dynamic>(sql).GroupBy(t => t.TableName)
						.Select(g => new TableInfo
						{
							Name = (string)g.Key,
							KeyColumns = g.Where(c => (int)c.PrimaryKey > 0).Select(c => (string)c.ColumnName).ToList(),
							OtherColumns = g.Where(c => (int)c.PrimaryKey == 0).Select(c => (string)c.ColumnName).ToList()
						}).ToList();

					var fks = db.Fetch<ForeignKeyConstraint>(@"select obj.name AS ForeignKeyName,
                            ('[' + sch.name + '].[' + tab1.name + ']') TableName,
                            ('[' +  col1.name + ']') ColumnName,
                            ('[' + sch2.name + '].[' + tab2.name + ']') ReferencedTableName,
                            ('[' +  col2.name + ']') ReferencedColumnName
                        from sys.foreign_key_columns fkc
                        inner join sys.foreign_keys obj
                            on obj.object_id = fkc.constraint_object_id
                        inner join sys.tables tab1
                            on tab1.object_id = fkc.parent_object_id
                        inner join sys.schemas sch
                            on tab1.schema_id = sch.schema_id
                        inner join sys.columns col1
                            on col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
                        inner join sys.tables tab2
                            on tab2.object_id = fkc.referenced_object_id
                        inner join sys.schemas sch2
                            on tab2.schema_id = sch2.schema_id
                        inner join sys.columns col2
                            on col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
                        where obj.is_disabled = 0");

					foreach (var table in tables)
						table.ForeignKeyConstraints = fks.Where(f => f.TableName == table.Name).ToList();

					return tables;
				}
			}
			catch (Exception ex)
			{
				Log.Fatal(ex, "Error getting tables to replicate from source database");
				throw;
			}
		}

		private Database GetDatabase(string connectionString, DatabaseType databaseType = null)
		{
			var db = new Database(connectionString, databaseType ?? DatabaseType.SqlServer2005, System.Data.SqlClient.SqlClientFactory.Instance);

			if (Timeout != 0) db.CommandTimeout = Timeout;

			return db;
		}

		private void Replicate(DatabaseInfo source, IGrouping<long, DatabaseInfo> destinations, IList<TableInfo> tables)
		{
			var changeInfo = RetrieveChanges(source, destinations, tables);
			if (changeInfo == null) return;

			// replicate changes to destinations
			foreach (var destination in destinations)
			{
				try
				{
					Log.Info($"Replicating {"change".ToQuantity(changeInfo.Changes.Count)} to destination {destination.Name}");

					using (var db = GetDatabase(destination.ConnectionString, DatabaseType.SqlServer2005))
					using (var transaction = db.GetTransaction(System.Data.IsolationLevel.ReadUncommitted))
					{
						try
						{
							var changes = changeInfo.Changes;
							var disabledForeignKeyConstraints = new Dictionary<ForeignKeyConstraint, long>();

							for (int i = 0; i < changes.Count; i++)
							{
								var change = changes[i];
								// Log.Debug($"Replicating change #{i + 1} of {changes.Count} (Version {change.Version}, CreationVersion {change.CreationVersion})");

								foreach (var fk in change.ForeignKeyConstraintsToDisable)
								{
									if (disabledForeignKeyConstraints.TryGetValue(fk.Key, out long untilVersion))
									{
										// FK is already disabled, check if it needs to be deferred further than currently planned
										if (fk.Value > untilVersion)
											disabledForeignKeyConstraints[fk.Key] = fk.Value;
									}
									else
									{
										DisableForeignKeyConstraint(db, fk.Key);
										disabledForeignKeyConstraints[fk.Key] = fk.Value;
									}
								}

								PerformChange(db, change);

								if ((i + 1) >= changes.Count || changes[i + 1].CreationVersion > change.CreationVersion) // there may be more than one change with the same CreationVersion
								{
									foreach (var fk in disabledForeignKeyConstraints.Where(f => f.Value <= change.CreationVersion).Select(f => f.Key).ToList())
									{
										ReenableForeignKeyConstraint(db, fk);
										disabledForeignKeyConstraints.Remove(fk);
									}
								}
							}

							if (!DryRun)
							{
								SetSyncVersion(db, changeInfo.Version);
								transaction.Complete();
							}

							Log.Info($"Destination {destination.Name} now at version {changeInfo.Version}");
						}
						catch (Exception ex)
						{
							Error = true;
							Log.Error(ex, $"Error replicating changes to destination {destination.Name}");
						}
					}
				}
				catch (Exception ex)
				{
					Error = true;
					Log.Error(ex, $"Error replicating changes to destination {destination.Name}");
				}
			}
		}

		private void ReenableForeignKeyConstraint(Database db, ForeignKeyConstraint fk)
		{
			Log.Debug($"Re-enabling foreign key constraint {fk.ForeignKeyName}");
			var sql = $"alter table {fk.TableName} with check check constraint {fk.ForeignKeyName}";
			if (!DryRun)
				db.Execute(sql);
		}

		private void DisableForeignKeyConstraint(Database db, ForeignKeyConstraint fk)
		{
			Log.Debug($"Disabling foreign key constraint {fk.ForeignKeyName}");
			var sql = $"alter table {fk.TableName} nocheck constraint {fk.ForeignKeyName}";
			if (!DryRun)
				db.Execute(sql);
		}

		private void SetSyncVersion(Database db, long currentVersion)
		{
			if (!DryRun)
			{
				var syncInfoTableExists = db.ExecuteScalar<string>("select top(1) name from sys.tables where name ='SyncInfo'") != null;

				if (!syncInfoTableExists)
				{
					db.Execute("create table SyncInfo (Id int not null primary key default 1 check (Id = 1), Version bigint not null)");
					db.Execute("insert into SyncInfo (Version) values (@0)", currentVersion);
				}
				else
				{
					db.Execute("update SyncInfo set Version = @0", currentVersion);
				}
			}
		}

		private ChangeInfo RetrieveChanges(DatabaseInfo source, IGrouping<long, DatabaseInfo> destinations, IList<TableInfo> tables)
		{
			var destinationVersion = destinations.Key;
			var changeInfo = new ChangeInfo();
			var changes = new List<Change>();

			using (var db = GetDatabase(source.ConnectionString, DatabaseType.SqlServer2008))
			{
				var snapshotIsolationEnabled = db.ExecuteScalar<int>("select snapshot_isolation_state from sys.databases where name = DB_NAME()") == 1;
				if (snapshotIsolationEnabled)
				{
					Log.Info($"Snapshot isolation is enabled in database {source.Name}");
					db.BeginTransaction(System.Data.IsolationLevel.Snapshot);
				}
				else
					Log.Info($"Snapshot isolation is not enabled in database {source.Name}, ignoring all changes above current version");

				changeInfo.Version = GetChangeTrackingCurrentVersion(db).Value;
				Log.Info($"Current version of database {source.Name} is {changeInfo.Version}");

				foreach (var table in tables)
				{
					var tableName = table.Name;
					var minVersion = db.ExecuteScalar<long?>("select CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@0))", tableName);

					Log.Info($"Minimum version of table {tableName} in database {source.Name} is {minVersion}");

					if (minVersion > destinationVersion)
					{
						Log.Error($"Cannot replicate table {tableName} to {"destination".ToQuantity(destinations.Count(), ShowQuantityAs.None)} {string.Join(", ", destinations.Select(d => d.Name))} because minimum source version {minVersion} is greater than destination version {destinationVersion}");
						Error = true;
						return null;
					}

					var sql = $@"select c.SYS_CHANGE_OPERATION, c.SYS_CHANGE_VERSION, c.SYS_CHANGE_CREATION_VERSION,
                        {string.Join(", ", table.KeyColumns.Select(c => "c." + c).Concat(table.OtherColumns.Select(c => "t." + c)))}
                        from CHANGETABLE (CHANGES {tableName}, @0) c
                        left outer join {tableName} t on ";
					sql += string.Join(" and ", table.KeyColumns.Select(k => $"c.{k} = t.{k}"));
					sql += " order by coalesce(c.SYS_CHANGE_CREATION_VERSION, c.SYS_CHANGE_VERSION)";

					// Log.Debug($"Retrieving changes for table {tableName}: {sql}");

					db.OpenSharedConnection();
					var cmd = db.CreateCommand(db.Connection, System.Data.CommandType.Text, sql, destinationVersion);

					using (var reader = cmd.ExecuteReader())
					{
						var numChanges = 0;

						while (reader.Read())
						{
							var col = 0;
							var change = new Change { Operation = ((string)reader[col])[0], Table = table };
							col++;
							var version = reader.GetInt64(col);
							change.Version = version;
							col++;
							var creationVersion = reader.IsDBNull(col) ? version : reader.GetInt64(col);
							change.CreationVersion = creationVersion;
							col++;

							if (!snapshotIsolationEnabled && Math.Min(version, creationVersion) > changeInfo.Version)
							{
								Log.Warn($"Ignoring change version {Math.Min(version, creationVersion)}");
								continue;
							}

							for (int i = 0; i < table.KeyColumns.Count; i++, col++)
								change.Keys[table.KeyColumns[i]] = reader.GetValue(col);
							for (int i = 0; i < table.OtherColumns.Count; i++, col++)
								change.Others[table.OtherColumns[i]] = reader.GetValue(col);

							changes.Add(change);
							numChanges++;
						}

						Log.Info($"Table {tableName} has {"change".ToQuantity(numChanges)}");
					}
				}

				if (snapshotIsolationEnabled)
					db.CompleteTransaction();
			}

			changeInfo.Changes.AddRange(changes.OrderBy(c => c.CreationVersion).ThenBy(c => c.Table.Name));

			ComputeForeignKeyConstraintsToDisable(changeInfo);

			return changeInfo;
		}

		private void ComputeForeignKeyConstraintsToDisable(ChangeInfo changeInfo)
		{
			var changes = changeInfo.Changes;

			for (int i = 0; i < changes.Count; i++)
			{
				var change = changes[i];
				if (change.CreationVersion < change.Version) // was inserted then later updated
				{
					for (int j = i + 1; j < changes.Count; j++)
					{
						var intermediateChange = changes[j];
						if (intermediateChange.CreationVersion > change.Version) // created later than last update to change
							break;
						if (intermediateChange.Operation != 'I') continue;

						// let's look at intermediateChange if it collides with change
						foreach (var fk in change.Table.ForeignKeyConstraints.Where(f => f.ReferencedTableName == intermediateChange.Table.Name))
						{
							var val = change.GetValue(fk.ColumnName);
							var refVal = intermediateChange.GetValue(fk.ReferencedColumnName);
							if (val != null && val.Equals(refVal))
							{
								// this foreign key constraint needs to be disabled
								Log.Info($"Foreign key constraint {fk.ForeignKeyName} needs to be disabled for change #{i + 1} from version {change.CreationVersion} until version {intermediateChange.CreationVersion}");
								change.ForeignKeyConstraintsToDisable[fk] = intermediateChange.CreationVersion;
							}
						}
					}
				}
			}
		}

		private void PerformChange(Database db, Change change)
		{
			var table = change.Table;
			var tableName = table.Name;
			var operation = change.Operation;

			switch (operation)
			{
				// Insert
				case 'I':
					var insertColumnNames = change.GetColumnNames();
					var insertSql = $"set IDENTITY_INSERT {tableName} ON; " +
						string.Format("insert into {0} ({1}) values ({2}); ", tableName,
						string.Join(", ", insertColumnNames),
						string.Join(", ", Parameters(insertColumnNames.Count))) +
						$"set IDENTITY_INSERT {tableName} OFF";
					var insertValues = change.GetValues();
					// Log.Debug($"Executing insert: {insertSql} ({FormatArgs(insertValues)})");
					if (!DryRun)
						db.Execute(insertSql, insertValues);
					break;

				// Update
				case 'U':
					var updateColumnNames = change.Others.Keys.ToList();
					var updateSql = string.Format("update {0} set {1} where {2}", tableName,
						string.Join(", ", updateColumnNames.Select((c, i) => $"{c} = @{i + change.Keys.Count}")),
						PrimaryKeys(table, change));
					var updateValues = change.GetValues();
					// Log.Debug($"Executing update: {updateSql} ({FormatArgs(updateValues)})");
					if (!DryRun)
						db.Execute(updateSql, updateValues);
					break;

				// Delete
				case 'D':
					var deleteSql = string.Format("delete from {0} where {1}", tableName, PrimaryKeys(table, change));
					var deleteValues = change.Keys.Values.ToArray();
					// Log.Debug($"Executing delete: {deleteSql} ({FormatArgs(deleteValues)})");
					if (!DryRun)
						db.Execute(deleteSql, deleteValues);
					break;
			}
		}

		private static string FormatArgs(object[] args) => string.Join(", ", args.Select((a, i) => $"@{i} = {a}"));

		private static string PrimaryKeys(TableInfo table, Change change) =>
			string.Join(" and ", change.Keys.Keys.Select((c, i) => $"{c} = @{i}"));

		private static IEnumerable<string> Parameters(int n) => Enumerable.Range(0, n).Select(c => "@" + c);

		private long GetCurrentVersion(DatabaseInfo dbInfo)
		{
			try
			{
				using (var db = GetDatabase(dbInfo.ConnectionString, DatabaseType.SqlServer2005))
				{
					var syncInfoTableExists = db.ExecuteScalar<string>("select top(1) name from sys.tables where name ='SyncInfo'") != null;
					long currentVersion;

					if (!syncInfoTableExists)
					{
						Log.Info($"SyncInfo table does not exist in database {dbInfo.Name}");
						currentVersion = GetChangeTrackingCurrentVersion(db) ?? -1;
						if (currentVersion < 0)
						{
							Log.Info($"Change tracking not enabled in database {dbInfo.Name}, assuming version 0");
							currentVersion = 0;
						}
						else
							Log.Info($"Database {dbInfo.Name} is at version {currentVersion}");
					}
					else
					{
						currentVersion = db.ExecuteScalar<long>("select top(1) Version from SyncInfo");
						Log.Info($"Database {dbInfo.Name} is at version {currentVersion}");
					}

					return currentVersion;
				}
			}
			catch (Exception ex)
			{
				Log.Error(ex, $"Error getting current version of destination database {dbInfo.Name}. Skipping this destination.");
				Error = true;
				return -1;
			}
		}

		/// <summary>
		/// Gets the current version from the source db
		/// </summary>
		/// <param name="connectionString"></param>
		/// <returns></returns>
		public long? GetChangeTrackingCurrentVersion(string connectionString)
		{
			using (var db = GetDatabase(connectionString, DatabaseType.SqlServer2008))
				return GetChangeTrackingCurrentVersion(db);
		}
		/// <summary>
		/// Gets the current version from the source db
		/// </summary>
		/// <param name="db"></param>
		/// <returns></returns>
		public long? GetChangeTrackingCurrentVersion(Database db)
		{
			return db.ExecuteScalar<long?>("select CHANGE_TRACKING_CURRENT_VERSION()");
		}

		/// <summary>
		/// Returns true if change tracking is enabled on source database
		/// </summary>
		/// <param name="connectionString"></param>
		/// <returns></returns>
		public bool GetChangeTrackingEnabled(string connectionString)
		{
			Console.WriteLine(connectionString);
			using (var db = GetDatabase(connectionString, DatabaseType.SqlServer2008))
				return db.Query<object>(@"
                    select null
                    from sys.change_tracking_databases
                    where database_id = db_id()").Any();
		}

		/// <summary>
		/// Returns the list of tables that change tracking is turned on for
		/// </summary>
		/// <param name="connectionString"></param>
		/// <returns></returns>
		public List<string> GetChangeTrackingEnabledTables(string connectionString)
		{
			var sql = @"
                SELECT quotename(s.name) + '.' + quotename(t.name)
                FROM sys.change_tracking_tables tr
                INNER JOIN sys.tables t on t.object_id = tr.object_id
                INNER JOIN sys.schemas s on s.schema_id = t.schema_id
                order by 1";
			using (var db = GetDatabase(connectionString, DatabaseType.SqlServer2008))
				return db.Query<string>(sql).ToList();
		}
	}
}

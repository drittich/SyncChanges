using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

using Dapper;

namespace SyncChanges
{
	public class Sql
	{
		/// <summary>
		/// Returns an open db connection for the given connection string
		/// </summary>
		/// <param name="connectionString"></param>
		/// <returns></returns>
		public static IDbConnection GetConnection(string connectionString)
		{
			if (string.IsNullOrWhiteSpace(connectionString))
				throw new Exception("Db connection string is empty");

			var cn = new SqlConnection(connectionString);
			cn.Open();

			return cn;
		}

		/// <summary>
		/// Object names must include schema and may include quote delimiters ([])
		/// This normalizes names so we can string compare for equivalence.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="changeToSchema"></param>
		/// <returns>A string like '[dbo].[myobjectname]'</returns>
		public static string NormalizeObjectName(string name, string changeToSchema)
		{
			if (!name.Contains("."))
				throw new Exception("name must include schema");

			// remove all square bracket so we start from a known place
			var normalizedName = name.Replace("[", "").Replace("]", "").ToLowerInvariant();

			var aName = normalizedName.Split(new char[] { '.' });
			var schema = aName[0];
			var nameOnly = aName[1];

			if (!string.IsNullOrWhiteSpace(changeToSchema))
				schema = changeToSchema;

			return $"[{schema}].[{nameOnly}]";
		}

		/// <summary>
		/// Compare object names for equality, taking into account that they may be in different schemas.
		/// </summary>
		/// <param name="name1"></param>
		/// <param name="name2"></param>
		/// <param name="changeToSchema"></param>
		/// <returns></returns>
		public static bool ObjectNamesAreEqual(string name1, string name2, string changeToSchema)
		{
			return NormalizeObjectName(name1, changeToSchema) == NormalizeObjectName(name2, changeToSchema);
		}

		/// <summary>
		/// Returns the db name from the connection string
		/// </summary>
		/// <param name="connectionString"></param>
		/// <returns></returns>
		public static string GetDbNameFromConnectionString(string connectionString)
		{
			var builder = new SqlConnectionStringBuilder(connectionString);

			//string server = builder.DataSource;
			string database = builder.InitialCatalog;

			return database;
		}

		/// <summary>
		/// Gets the number of rows in the given table
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public static int GetTableRowCount(string connectionString, string table)
		{
			var sql = $@"select count(*) from {NormalizeObjectName(table, null)}";
			using (var cn = Sql.GetConnection(connectionString))
				return cn.Query<int>(sql).Single();
		}

		/// <summary>
		/// Making use of SQL Change Tracking, does the initial data
		/// population to bring the source and destination tables in sync
		/// </summary>
		/// <param name="sourceConnectionString"></param>
		/// <param name="destinationConnectionString"></param>
		/// <param name="table"></param>
		public static void DoInitialDataPopulationForTable(string sourceConnectionString, string destinationConnectionString, string table)
		{
			throw new NotImplementedException();
		}
	}
}

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

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
		/// <returns>A string like '[dbo].[myobjectname]'</returns>
		public static string NormalizeObjectName(string name, string changeToSchema)
		{
			if (!name.Contains("."))
				throw new Exception("name must include schema");

			// remove all square bracket so we start from a known place
			var normalizedName = name.Replace("[", "").Replace("]", "").ToLowerInvariant();

			var aName = name.Split(new char[] { '.' });
			var schema = aName[0];
			var nameOnly = aName[1];

			if (!string.IsNullOrWhiteSpace(changeToSchema))
				schema = changeToSchema;

			return $"[{schema}].[{nameOnly}]";
		}
	}
}

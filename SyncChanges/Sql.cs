using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace SyncChanges
{
	public class Sql
	{
		public static IDbConnection GetConnection(string connectionString)
		{
			if (string.IsNullOrWhiteSpace(connectionString))
				throw new Exception("Db connection string is empty");

			var cn = new SqlConnection(connectionString);
			cn.Open();

			return cn;
		}

		/// <summary>
		/// Object names may or may not include schema or quote delimiters ([])
		/// This normalizes names so we can string compare for equivalence.
		/// </summary>
		/// <param name="name"></param>
		/// <returns>A string like '[dbo].[myobjectname]'</returns>
		public static string NormalizeObjectName(string name, string changeToSchema)
		{
			// Let's assume no one would name an object with a period
			var hasSchema = name.Contains(".");

			if (!hasSchema && string.IsNullOrWhiteSpace(changeToSchema))
				throw new Exception("Schema must be supplied as part of name or via changeToSchema paramter");

			;

				// remove all square bracket so we start from a known place
				var normalizedName = name.Replace("[", "").Replace("]", "").ToLowerInvariant();


			if (name1 == name)
		}

	}
}

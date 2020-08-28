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

	}
}

using NPoco;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit;
using NUnit.Framework;
using SyncChanges;
using System.Threading;

namespace SyncChanges.Tests
{
	[TestFixture]
	public class AutoPilotTests
	{
		const string ConnectionString = @"Server=(localdb)\MSSQLLocalDB;Integrated Security=true";
		const string SourceDatabaseName = "SyncChangesTestSource";
		const string DestinationDatabaseName = "SyncChangesTestDestination";

		static string GetConnectionString(string db = "") => ConnectionString + (db != "" ? $";Initial Catalog={db}" : "");
		static Database GetDatabase(string db = "") => new Database(GetConnectionString(db), DatabaseType.SqlServer2012);

		static void DropDatabase(string name)
		{
			using (var db = GetDatabase())
			{
				var sql = $@"if (exists(select name from master.dbo.sysdatabases where name = '{name}'))
                begin
                    alter database [{name}]
                    set single_user with rollback immediate
                    drop database [{name}]
                end";
				db.Execute(sql);
			}
		}

		private static void CreateDatabase(string name)
		{
			using (var db = GetDatabase())
			{
				db.Execute($"create database [{name}]");
				db.Execute($"alter database [{name}] set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)");
				if ((string)TestContext.CurrentContext.Test.Properties.Get("snapshot") != "off")
				{
					db.Execute($"ALTER DATABASE [{name}] SET ALLOW_SNAPSHOT_ISOLATION ON");
					db.Execute($"ALTER DATABASE [{name}] SET READ_COMMITTED_SNAPSHOT ON");
				}
			}
		}

		[SetUp]
		public void Setup()
		{
			DropDatabase(SourceDatabaseName);
			DropDatabase(DestinationDatabaseName);
			CreateDatabase(SourceDatabaseName);
			CreateDatabase(DestinationDatabaseName);
		}

		[TearDown]
		public void TearDown()
		{
			DropDatabase(SourceDatabaseName);
			DropDatabase(DestinationDatabaseName);
		}

		void CreateUsersTable(string dbName)
		{
			using (var db = GetDatabase(dbName))
			{
				db.Execute(@"if not exists (select * from sys.tables where name = 'Users') create table Users (
                    UserId int identity(1,1) primary key not null,
                    Name nvarchar(200) null,
                    Age int null,
                    DateOfBirth datetime null,
                    Savings decimal null
                )");
				db.Execute(@"alter table Users
                    enable CHANGE_TRACKING
                    with (TRACK_COLUMNS_UPDATED = OFF)");
			}
		}

		void CreateOrdersTable(string dbName)
		{
			using (var db = GetDatabase(dbName))
			{
				db.Execute(@"if not exists (select * from sys.tables where name = 'Orders') create table Orders (
                    OrderId int identity(1,1) primary key not null,
                    UserId int not null
                )");
				db.Execute(@"alter table Orders
                    enable CHANGE_TRACKING
                    with (TRACK_COLUMNS_UPDATED = OFF)");
			}
		}

		void CreateOrdersForeignKey(string dbName)
		{
			using (var db = GetDatabase(dbName))
				db.Execute(@"alter table Orders add constraint Orders_UserId_FK foreign key (UserId) references Users(UserId)");
		}

		void DropTable(string dbName, string tableName)
		{
			using (var db = GetDatabase(dbName))
				db.Execute($@"if exists (select * from sys.tables where name = '{tableName}') drop table {tableName}");
		}

		void CreateUsersTable()
		{
			DropTable("Users");
			CreateUsersTable(SourceDatabaseName);
			CreateUsersTable(DestinationDatabaseName);
		}

		void CreateOrdersTable()
		{
			DropTable("Orders");
			CreateOrdersTable(SourceDatabaseName);
			CreateOrdersTable(DestinationDatabaseName);
		}

		void DropTable(string tableName)
		{
			DropTable(SourceDatabaseName, tableName);
			DropTable(DestinationDatabaseName, tableName);
		}

		[TableName("Users")]
		[PrimaryKey("UserId")]
		class User
		{
			public int UserId { get; set; }
			public string Name { get; set; }
			public int Age { get; set; }
			public DateTime DateOfBirth { get; set; }
			public decimal Savings { get; set; }

			public override bool Equals(Object obj)
			{
				if (obj == null || GetType() != obj.GetType())
					return false;

				User u = (User)obj;
				return UserId == u.UserId && Name == u.Name
					&& Age == u.Age && DateOfBirth == u.DateOfBirth && Savings == u.Savings;
			}

			public override int GetHashCode()
			{
				return UserId;
			}
		}

		[TableName("Orders")]
		[PrimaryKey("OrderId")]
		class Order
		{
			public int OrderId { get; set; }
			public int UserId { get; set; }

			public override bool Equals(Object obj)
			{
				if (obj == null || GetType() != obj.GetType())
					return false;

				Order o = (Order)obj;
				return OrderId == o.OrderId && UserId == o.UserId;
			}

			public override int GetHashCode()
			{
				return OrderId;
			}
		}

		readonly ReplicationSet TestReplicationSet = new ReplicationSet
		{
			Name = "Test",
			Source = new DatabaseInfo { Name = "Source", ConnectionString = GetConnectionString(SourceDatabaseName) },
			Destinations = { new DatabaseInfo { Name = "Destination", ConnectionString = GetConnectionString(DestinationDatabaseName) } },
			Tables = { "Users", "dbo.Orders" }
		};

		Config TestConfig { get; set; }

		public AutoPilotTests()
		{
			TestConfig = new Config { ReplicationSets = { TestReplicationSet } };
		}

		[Test]
		public void CanDetectChangeTrackingEnabled() { Assert.Fail(); }
		[Test]
		public void CanDetectTableChangeTrackingEnabled() { Assert.Fail(); }
		[Test]
		public void CanDetectSyncChangesTableInitialized() { Assert.Fail(); }
		[Test]
		public void CanDetectSourceSyncVersion() { Assert.Fail(); }
		[Test]
		public void CanDetectDestinationSyncVersion() { Assert.Fail(); }
		[Test]
		public void CanDetectDestinationTableExists() { Assert.Fail(); }
		[Test]
		public void CanDetectDestinationTablePopulated() { Assert.Fail(); }
		[Test]
		public void CanDoInitialPopulation() { Assert.Fail(); }
		[Test]
		public void CanNormalizeObjectNames()
		{
			var tests = new (string, string, string)[] {
				("dbo.User", "target", "[target].[user]"),
				("dbo.User", null, "[dbo].[user]")
			};
			foreach (var test in tests)
				Assert.AreEqual(Sql.NormalizeObjectName(test.Item1, test.Item2), test.Item3);
		}
		[Test]
		public void CanCompareObjectNames() {

			Assert.IsTrue(Sql.ObjectNamesAreEqual("dbo.User", "dbo.user", "target"));
		}
	}
}

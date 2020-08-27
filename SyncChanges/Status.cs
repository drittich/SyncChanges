using NLog;

using System;
using System.Collections.Generic;
using System.Text;

namespace SyncChanges
{
	class Status
	{
		static Logger Log = LogManager.GetCurrentClassLogger();
		Config Config { get; set; }

		public bool ChangeTrackingEnabled { get; set; }
		public List<string> ChangeTrackingEnabledTables { get; set; }
		public long CurrentChangeTrackingVersion { get; set; }

		public Status(Config config)
		{
			Config = config ?? throw new ArgumentException("config is null", nameof(config));
		}
	}
}

﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

using Mono.Options;

using Newtonsoft.Json;

using NLog;

namespace SyncChanges.Console
{
	class Program
	{
		static Logger Log = LogManager.GetCurrentClassLogger();

		List<string> ConfigFiles;
		bool DryRun;
		bool Error = false;
		int Timeout = 0;
		bool Loop = false;
		int Interval = 30;

		static int Main(string[] args)
		{
			OptionSet options;

			try
			{
				System.Console.OutputEncoding = Encoding.UTF8;
				var program = new Program();
				var showHelp = false;
				var showStatus = false;
				var verboseLogging = false;

				try
				{
					/*
                     * New Options:
                     * - Enable Change Tracking on source
                     * - Disable Change Tracking on source
                     * - Reset Change Tracking on source (back to 0?)
                     * - Enable Change Tracking on source related tables
                     * - Disable Change Tracking on source non-related tables
                     * - Create tables in destination(s)
                     * - Do initial data population
                     * - Sync required views
                     * - Sync required TVFs
                     * - Status: 
                     *      - Is change tracking enabled on source?
                     *      - What tables have change-tracking enabled?
                     *      - What version are we at?
                     *      - Whether tables exist and are populated on destination
                     */

					options = new OptionSet {
						{ "h|help", "Show this message and exit", v => showHelp = v != null },
						{ "d|dryrun", "Do not alter target databases, only perform a test run", v => program.DryRun = v != null },
						{ "t|timeout=", "Database command timeout in seconds", (int v) => program.Timeout = v },
						{ "l|loop", "Perform replication in a loop, periodically checking for changes", v => program.Loop = v != null },
						{ "i|interval=", "Replication interval in seconds (default is 30); only relevant in loop mode", (int v) => program.Interval = v },
						{ "s|status", "Show status and exit", v => showStatus = v != null },
						{ "v|verbose", "Verbose logging", v => verboseLogging = v != null },
					};
					program.ConfigFiles = options.Parse(args);
					if (showHelp)
					{
						ShowHelp(options);
						return 0;
					}
					if (showStatus)
					{
						program.ShowStatus(options, verboseLogging);
						return 0;
					}
				}
				catch (Exception ex)
				{
					Log.Error(ex, "Error parsing command line arguments");
					return 1;
				}

				if (!program.ConfigFiles.Any())
				{
					Log.Error("No config files supplied");
					ShowHelp(options);
					return 1;
				}

				program.Sync();

				return program.Error ? 1 : 0;
			}
			catch (Exception ex)
			{
				Log.Error(ex, "An error has occurred");

				return 2;
			}
		}

		void ShowStatus(OptionSet options, bool verboseLogging)
		{
			foreach (var configFile in ConfigFiles)
			{
				Config config = null;

				try
				{
					config = JsonConvert.DeserializeObject<Config>(File.ReadAllText(configFile));
				}
				catch (Exception ex)
				{
					Log.Error(ex, $"Error reading configuration file {configFile}");
					Error = true;
					continue;
				}

				try
				{
					var synchronizer = new Synchronizer(config) { DryRun = DryRun, Timeout = Timeout };
					var listSeparator = "\n\t";
					foreach (var replicationSet in config.ReplicationSets)
					{
						Log.Info($"Replication Set: {replicationSet.Name}");

						// is source change tracking enabled?
						var changeTrackingEnabled = synchronizer.GetChangeTrackingEnabled(replicationSet.Source.ConnectionString);
						Log.Info($"Change Tracking Enabled: {changeTrackingEnabled}");

						// what is source change tracking version?
						var currentVersion = synchronizer.GetChangeTrackingCurrentVersion(replicationSet.Source.ConnectionString);
						Log.Info($"Change Tracking Current Version: {(currentVersion == null ? "(none)" : currentVersion.ToString()) }");

						// what tables have change tracking enabled
						var enabledTables = synchronizer.GetChangeTrackingEnabledTables(replicationSet.Source.ConnectionString);
						LogListMessage(enabledTables, "Change Tracking Enabled Tables", verboseLogging);

						// what tables and views are we syncing?
						List<SyncObject> syncObjects = synchronizer.GetSyncObjectsWithDependencies(replicationSet);
						var syncViews = syncObjects.Where(o => o.Type == SyncObject.ObjectType.View).Select(o => o.Name);
						LogListMessage(syncViews, "Views To Sync", verboseLogging);

						var syncTables = syncObjects.Where(o => o.Type == SyncObject.ObjectType.Table).Select(o => o.Name);
						LogListMessage(syncTables, "Tables To Sync", verboseLogging);

						// these tables are requested to be synced, but do not have have
						// change tracking enabled
						var shouldBeEnabledButAreNot = syncTables.Where(t => !enabledTables.Any(e => e.ToLowerInvariant() == t.ToLowerInvariant()));
						LogListMessage(shouldBeEnabledButAreNot, "Tables to Sync Without Change Tracking enabled", verboseLogging);

						foreach (var destination in replicationSet.Destinations)
						{
							// are destinations set up with SyncVersion table?

							// Do destination tables exist?
							var nonExistingTables = synchronizer.GetNonExistingSyncTables(destination.ConnectionString, syncTables);
							LogListMessage(nonExistingTables, $"Tables to sync not existing in destination [{destination.Name}]", verboseLogging);

							// Are destination tables populated?

							// what are the destination sync versions
						}

						// what tables exist in the destination that we don't need?
					}
				}
				catch (Exception ex)
				{
					Log.Error(ex, $"Error getting status for configuration [{configFile}]");
					Error = true;
				}
			}
		}

		private void LogListMessage(IEnumerable<string> list, string v, bool verboseLogging)
		{
			var listSeparator = "\n\t";

			var msg = $"{v}: {list.Count()}";
			if (verboseLogging && list.Any())
				msg+= listSeparator + string.Join(listSeparator, list);
			Log.Info(msg);
		}

		static void ShowHelp(OptionSet p)
		{
			System.Console.WriteLine("Usage: SyncChanges [OPTION]... CONFIGFILE...");
			System.Console.WriteLine("Replicate database changes.");
			System.Console.WriteLine();
			System.Console.WriteLine("Options:");
			p.WriteOptionDescriptions(System.Console.Out);
		}

		void Sync()
		{
			foreach (var configFile in ConfigFiles)
			{
				Config config = null;

				try
				{
					config = JsonConvert.DeserializeObject<Config>(File.ReadAllText(configFile));
				}
				catch (Exception ex)
				{
					Log.Error(ex, $"Error reading configuration file {configFile}");
					Error = true;
					continue;
				}

				try
				{
					var synchronizer = new Synchronizer(config) { DryRun = DryRun, Timeout = Timeout };
					if (!Loop)
					{
						var success = synchronizer.Sync();
						Error = Error || !success;
					}
					else
					{
						synchronizer.Interval = Interval;
						var cancellationTokenSource = new CancellationTokenSource();
						System.Console.CancelKeyPress += (s, e) =>
						{
							cancellationTokenSource.Cancel();
							e.Cancel = true;
						};
						synchronizer.SyncLoop(cancellationTokenSource.Token);
					}
				}
				catch (Exception ex)
				{
					Log.Error(ex, $"Error synchronizing databases for configuration {configFile}");
					Error = true;
				}
			}
		}
	}
}

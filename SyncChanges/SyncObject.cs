using System;
using System.Collections.Generic;
using System.Text;

namespace SyncChanges
{
	public class SyncObject
	{
		public string Name { get; set; }
		public ObjectType Type { get; set; }

		public enum ObjectType
		{
			Table, View, Function
		}
	}
}

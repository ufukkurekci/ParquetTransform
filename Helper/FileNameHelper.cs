using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FormParquet.Helper
{
	public static class FileNameHelper
	{
		private static int counter = 1;
		private static string GlobalcounterString = string.Empty;
		public static void SetCounter()
		{
			string counterString = counter.ToString().PadLeft(4, '0');
			counter++;

			GlobalcounterString = counterString;
		}

		public static string GetDynamicFileName(string prefix)
		{
			//string prefix = "KK002_OKKIB_";
			string date = DateTime.Now.ToString("yyyy_MM_dd");

			string fileName = $"{prefix}{date}_{GlobalcounterString}.parquet";

			return fileName;
		}
	}
}

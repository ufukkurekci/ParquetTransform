namespace GlobalHelper
{
	public static class FileNameHelper
	{
		static int counter = 1;
		static string GetCounter()
		{
			string counterString = counter.ToString().PadLeft(4, '0');
			counter++;

			return counterString;
		}
		public static string GetDynamicFileName(string prefix)
		{
			string date = DateTime.Now.AddDays(-1).ToString("yyyy_MM_dd");
			string counterString = GetCounter();

			string fileName = $"{prefix}{date}_{counterString}.parquet";

			return fileName;
		}
	}
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GlobalHelper
{
	public static class ParametherHelper
	{
		public static DateTime? parametreToday;
		public static DateTime? parametreTomarrow;
		public static void DateRange(DateTime date1, DateTime date2, bool dateRangeChecked)
		{
			if (dateRangeChecked)
			{
				parametreToday = date1;
				parametreTomarrow = date2;
				return;
			}

			parametreToday = new DateTime(2022, 1, 1);
			parametreTomarrow = DateTime.Now;
		}
	}
}

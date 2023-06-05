﻿using EK3.NormalVersion;
using EK4.NormalVersion;
using EK5.NormalVersion;
using EK7.NormalVersion;
namespace FormParquet
{
	public partial class Form1 : Form
	{
		public Form1()
		{
			InitializeComponent();
		}

		private void textBox1_TextChanged(object sender, EventArgs e)
		{

		}

		private void label1_Click(object sender, EventArgs e)
		{

		}
		private async void ek3_button_Click(object sender, EventArgs e)
		{
			Helper.DateRange(today.Value.Date, tomarrow.Value.Date, dateRange.Checked);
			EK3_ParquetOperation.today = Helper.parametreToday;
			EK3_ParquetOperation.tomarrow = Helper.parametreTomarrow;

			EK3_ParquetOperation parquetOperation = new EK3_ParquetOperation();
			await parquetOperation.GetParquetFile();
			var filename = EK3_ParquetOperation.ek3filename;
			string message = $"{filename} adında dosya exe dizininde ek3_output klasoru altında olusturuldu";
			string top = "Islem Basarili !";
			MessageBox.Show(message, top);

		}
		private async void ek4_button_Click(object sender, EventArgs e)
		{
			Helper.DateRange(today.Value.Date, tomarrow.Value.Date, dateRange.Checked);
			EK4_ParquetOperation.today = Helper.parametreToday;
			EK4_ParquetOperation.tomarrow = Helper.parametreTomarrow;

			EK4_ParquetOperation parquetOperation = new EK4_ParquetOperation();
			await parquetOperation.GetParquetFile();
			var filename = EK4_ParquetOperation.ek4filename;
			string message = $"{filename} adında dosya exe dizininde ek4_output klasoru altında olusturuldu";
			string top = "Islem Basarili !";
			MessageBox.Show(message, top);
		}
		private async void ek5_button_Click(object sender, EventArgs e)
		{
			Helper.DateRange(today.Value.Date, tomarrow.Value.Date, dateRange.Checked);
			EK5_ParquetOperation.today = Helper.parametreToday;
			EK5_ParquetOperation.tomarrow = Helper.parametreTomarrow;

			EK5_ParquetOperation parquetOperation = new EK5_ParquetOperation();
			await parquetOperation.GetParquetFile();
			var filename = EK5_ParquetOperation.ek5filename;
			string message = $"{filename} adında dosya exe dizininde ek5_output klasoru altında olusturuldu";
			string top = "Islem Basarili !";
			MessageBox.Show(message, top);
		}

		private async void ek7_button_Click(object sender, EventArgs e)
		{
			Helper.DateRange(today.Value.Date, tomarrow.Value.Date, dateRange.Checked);
			EK7_ParquetOpertion.today = Helper.parametreToday;
			EK7_ParquetOpertion.tomarrow = Helper.parametreTomarrow;

			EK7_ParquetOpertion parquetOperation = new EK7_ParquetOpertion();
			await parquetOperation.GetParquetFile();
			var filename = EK7_ParquetOpertion.ek7filename;
			string message = $"{filename} adında dosya exe dizininde ek7_output klasoru altında olusturuldu";
			string top = "Islem Basarili !";
			MessageBox.Show(message, top);
		}

		private void Form1_Load(object sender, EventArgs e)
		{
			today.Enabled = false;
			tomarrow.Enabled = false;
		}

		private void dateRange_CheckedChanged(object sender, EventArgs e)
		{
			today.Enabled = dateRange.Checked;
			tomarrow.Enabled = dateRange.Checked;
		}
	}
}
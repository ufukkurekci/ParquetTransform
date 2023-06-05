using EK3.NormalVersion;
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
			EK3_ParquetOperation parquetOperation = new EK3_ParquetOperation();
			await parquetOperation.GetParquetFile();
			var filename = EK3_ParquetOperation.ek3filename;
			string message = $"{filename} adında dosya exe dizininde ek3_output klasoru altında olusturuldu";
			string top = "Islem Basarili !";
			//await Task.Delay(200);
			MessageBox.Show(message, top);

		}
		private async void ek4_button_Click(object sender, EventArgs e)
		{
			EK4_ParquetOperation parquetOperation = new EK4_ParquetOperation();
			await parquetOperation.GetParquetFile();
			var filename = EK4_ParquetOperation.ek4filename;
			string message = $"{filename} adında dosya exe dizininde ek4_output klasoru altında olusturuldu";
			string top = "Islem Basarili !";
			//await Task.Delay(200);
			MessageBox.Show(message, top);
		}
		private async void ek5_button_Click(object sender, EventArgs e)
		{
			EK5_ParquetOperation parquetOperation = new EK5_ParquetOperation();
			await parquetOperation.GetParquetFile();
			var filename = EK5_ParquetOperation.ek5filename;
			string message = $"{filename} adında dosya exe dizininde ek5_output klasoru altında olusturuldu";
			string top = "Islem Basarili !";
			//await Task.Delay(200);
			MessageBox.Show(message, top);
		}

		private async void ek7_button_Click(object sender, EventArgs e)
		{
			EK7_ParquetOpertion parquetOperation = new EK7_ParquetOpertion();
			await parquetOperation.GetParquetFile();
			var filename = EK7_ParquetOpertion.ek7filename;
			string message = $"{filename} adında dosya exe dizininde ek7_output klasoru altında olusturuldu";
			string top = "Islem Basarili !";
			//await Task.Delay(200);
			MessageBox.Show(message, top);
		}

		private void Form1_Load(object sender, EventArgs e)
		{

		}
	}
}
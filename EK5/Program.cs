using EK5.NormalVersion;

//EK5_ParquetOperation parquetOperation = new EK5_ParquetOperation();
//Console.WriteLine("Dosya olusturuluyor ..");
//parquetOperation.GetParquetFile();

//ek5_validation validation = new ek5_validation();
//Console.WriteLine("Dosya olusturuluyor ");
//validation.validation5();

EK5_ParquetOperation.today = string.Format("{0:yyyyMMdd}", DateTime.Now.AddDays(-1));
EK5_ParquetOperation.tomarrow = string.Format("{0:yyyyMMdd}", DateTime.Now);

EK5_ParquetOperation parquetOperation = new EK5_ParquetOperation();
await parquetOperation.GetParquetFile();

Console.WriteLine($"{EK5_ParquetOperation.ek5filename} adında dosya exe dizininde parquet klasörü altında olusturuldu ve sftp ye yüklendi.");
Console.ReadLine();

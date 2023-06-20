using EK3.NormalVersion;

//Console.WriteLine("Dosya olusturuluyor ..");



EK3_ParquetOperation.today = string.Format("{0:yyyyMMdd}", DateTime.Now.AddDays(-1));
EK3_ParquetOperation.tomarrow = string.Format("{0:yyyyMMdd}", DateTime.Now);

EK3_ParquetOperation parquetOperation = new EK3_ParquetOperation();
await parquetOperation.GetParquetFile();

//ek3_validation validation = new ek3_validation();
//Console.WriteLine("Dosya olusturuluyor ..");
//validation.validation3();

Console.WriteLine($"{EK3_ParquetOperation.ek3filename} adında dosya exe dizininde parquet klasörü altında olusturuldu ve sftp ye yüklendi.");
Console.ReadLine();


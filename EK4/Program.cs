using EK4.NormalVersion;

//EK4_ParquetOperation parquetOperation = new EK4_ParquetOperation();
//Console.WriteLine("Dosya olusturuluyor ..");
//parquetOperation.GetParquetFile();


//ek4_validation validation = new ek4_validation();
//Console.WriteLine("Dosya olusturuluyor ..");
//validation.validation4();

EK4_ParquetOperation.today = string.Format("{0:yyyyMMdd}", DateTime.Now.AddDays(-1));
EK4_ParquetOperation.tomarrow = string.Format("{0:yyyyMMdd}", DateTime.Now);

EK4_ParquetOperation parquetOperation = new EK4_ParquetOperation();
await parquetOperation.GetParquetFile();

Console.WriteLine($"{EK4_ParquetOperation.ek4filename} adında dosya exe dizininde parquet klasörü altında olusturuldu ve sftp ye yüklendi.");
Console.ReadLine();


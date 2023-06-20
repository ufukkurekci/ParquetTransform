using EK7.NormalVersion;

//EK7_ParquetOpertion parquetOperation = new EK7_ParquetOpertion();
//Console.WriteLine("Dosya olusturuluyor ..");
//parquetOperation.GetParquetFile();

//ek7_validation validation = new ek7_validation();
//Console.WriteLine("Dosya olusturuluyor ..");
//validation.validation7();

EK7_ParquetOpertion.today = string.Format("{0:yyyyMMdd}", DateTime.Now.AddDays(-1));
EK7_ParquetOpertion.tomarrow = string.Format("{0:yyyyMMdd}", DateTime.Now);

EK7_ParquetOpertion parquetOperation = new EK7_ParquetOpertion();
await parquetOperation.GetParquetFile();

Console.WriteLine($"{EK7_ParquetOpertion.ek7filename} adında dosya exe dizininde parquet klasörü altında olusturuldu ve sftp ye yüklendi.");
Console.ReadLine();



using Microsoft.Data.SqlClient;
using ParquetSharp;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using Encoding = System.Text.Encoding;

namespace EK7.NormalVersion
{
    public class EK7_ParquetOpertion
    {
        public static string ek7filename = "";
        public static DateTime? today = null;
        public static DateTime? tomarrow = null;

		public async Task GetParquetFile()
        {
            // Veritabanı bağlantı dizesi
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            // UAT
   //         builder.DataSource = "172.25.84.24";
   //         builder.UserID = "quantra";
   //         builder.Password = "quantra2";
   //         builder.InitialCatalog = "EPara";
   //         builder.Encrypt = true;
   //         builder.TrustServerCertificate = true;
			//builder.CommandTimeout = 0;
			// UAT

			builder.DataSource = "PRD-SQL-ETUGRA1";
            builder.InitialCatalog = "EPara";
            builder.IntegratedSecurity = true;
            builder.TrustServerCertificate = true;
            builder.CommandTimeout = 0;


            // (EK7) Ödeme Kuruluşu Kart İşlem bilgileri formu           
            // KK002_OKKIB_2020_12_20_0001.parquet
            Random random = new Random();

            string sqlQuery = "dbo.GENERATE_PARQUET_EK7";

			using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {

                connection.Open();


                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {
					command.CommandType = CommandType.StoredProcedure;
					command.Parameters.AddWithValue("@TODAY", today);
					command.Parameters.AddWithValue("@TOMORROW", tomarrow);

					using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {

                        List<string> RecordType = new List<string>();
                        List<string> LocalRef = new List<string>();
                        List<string> İslemTuruKodu = new List<string>();
                        List<string> VergiKimlikNo = new List<string>();
                        List<string> TuzelKisilikUnvan = new List<string>();
                        List<string> Ad = new List<string>();
                        List<string> Soyad = new List<string>();
                        List<string> KimlikTipi = new List<string>();
                        List<string> KimlikNumarasi = new List<string>();
                        List<string> KartNumarasi = new List<string>();
                        List<string> Banktip = new List<string>();
                        List<string> BankEftKod = new List<string>();
                        List<string> BankAtmKod = new List<string>();
                        List<string> IslemTarihi = new List<string>();
                        List<string> islemTutar = new List<string>();
                        List<string> AsilParaTutari = new List<string>();
                        List<string> BirimISOCode = new List<string>();
                        List<string> Komisyon = new List<string>();
                        List<string> MusteriAciklama = new List<string>();
                        List<string> KurumAciklama = new List<string>();
                        List<string> kurumkod = new List<string>();

                        var columns = new Column[]
                        {
                            new Column<string>("recordtype"),
                            new Column<string>("lref"),
                            new Column<string>("islemturu"),
                            new Column<string>("ksahtkvkn"),
                            new Column<string>("ksahtkunvan"),
                            new Column<string>("ksahgkad"),
                            new Column<string>("ksahgksoyad"),
                            new Column<string>("ksahgkkimliktipi"),
                            new Column<string>("ksahgkkimlikno"),
                            new Column<string>("ksahkartno"),
                            new Column<string>("banktip"),
                            new Column<string>("bankeftkod"),
                            new Column<string>("bankatmkod"),
                            new Column<string>("istar"),
                            new Column<string>("islemtutar"),
                            new Column<string>("asiltutar"),
                            new Column<string>("parabirim"),
                            new Column<string>("brutkomtut"),
                            new Column<string>("musaciklama"),
                            new Column<string>("kuraciklama"),
                            new Column<string>("kurumkod")

                        };
                        #region row_reader_area
                        while (await reader.ReadAsync())
                        {
                            RecordType.Add(reader.GetString("recordtype"));
                            LocalRef.Add(reader.GetInt64("lref").ToString());
                            İslemTuruKodu.Add(reader.GetString("islemturu"));
                            VergiKimlikNo.Add(reader.GetString("ksahtkvkn"));
                            TuzelKisilikUnvan.Add(reader.GetString("ksahtkunvan"));
                            Ad.Add(reader.GetString("ksahgkad"));
                            Soyad.Add(reader.GetString("ksahgksoyad"));
                            KimlikTipi.Add(reader.GetInt32("ksahgkkimliktipi").ToString());
                            KimlikNumarasi.Add(reader.GetString("ksahgkkimlikno"));
                            KartNumarasi.Add(reader.GetString("ksahkartno"));
                            Banktip.Add(reader.GetString("banktip"));
                            BankEftKod.Add(reader.GetString("bankeftkod"));
                            BankAtmKod.Add(reader.GetString("bankatmkod"));
                            IslemTarihi.Add(reader.GetDateTime("istar").ToString());
                            islemTutar.Add(reader.GetDecimal("islemtutar").ToString());
                            AsilParaTutari.Add(reader.GetDecimal("asiltutar").ToString());
                            BirimISOCode.Add(reader.GetString("parabirim"));
                            Komisyon.Add(reader.GetString("brutkomtut"));
                            MusteriAciklama.Add(reader.GetString("musaciklama"));
                            KurumAciklama.Add(reader.GetString("kuraciklama"));
                            kurumkod.Add(reader.GetString("kurumkod"));

                        }
						#endregion

						ek7filename = GetDynamicFileName();

						using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ek7_output", ek7filename), columns);
                        using var rowGroup = file.AppendRowGroup();
                        #region parquet_writer

                        /*RecordType*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())  /*RecordType*/
                        {
                            objectIdWriter.WriteBatch(RecordType.ToArray());
                        }

                        /*LocalRef*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(LocalRef.ToArray());
                        }

                        /*İslemTuruKodu*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(İslemTuruKodu.ToArray());
                        }

                        /*VergiKimlikNo*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(VergiKimlikNo.ToArray());
                        }

                        /*TuzelKisilikUnvan*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(TuzelKisilikUnvan.ToArray());
                        }

                        /*Ad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Ad.ToArray());
                        }

                        /*Soyad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Soyad.ToArray());
                        }

                        /*KimlikTipi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KimlikTipi.ToArray());
                        }

                        /*KimlikNumarasi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KimlikNumarasi.ToArray());
                        }

                        /*KartNumarasi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KartNumarasi.ToArray());
                        }

                        /*Banktip*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Banktip.ToArray());
                        }

                        /*BankEftKod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(BankEftKod.ToArray());
                        }

                        /*BankAtmKod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(BankAtmKod.ToArray());
                        }

                        /*IslemTarihi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(IslemTarihi.ToArray());
                        }

                        /*islemTutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(islemTutar.ToArray());
                        }

                        /*AsilParaTutari*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(AsilParaTutari.ToArray());
                        }

                        /*BirimISOCode*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(BirimISOCode.ToArray());
                        }

                        /*Komisyon*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Komisyon.ToArray());
                        }

                        /*MusteriAciklama*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(MusteriAciklama.ToArray());
                        }

                        /*KurumAciklama*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KurumAciklama.ToArray());
                        }

                        /*kurumkod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(kurumkod.ToArray());
                        }


                        #endregion
                        file.Close();

                    }
                }
            }
        }

		static int counter = 1;
		static string GetCounter()
		{
			string counterString = counter.ToString().PadLeft(4, '0');
			counter++;

			return counterString;
		}

		public static string GetDynamicFileName()
		{
			string prefix = "KK002_OKKIB_";
			string date = DateTime.Now.ToString("yyyy_MM_dd");
			string counterString = GetCounter();

			string fileName = $"{prefix}{date}_{counterString}.parquet";

			return fileName;
		}
	}
}

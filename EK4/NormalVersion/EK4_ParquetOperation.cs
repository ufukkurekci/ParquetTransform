using Microsoft.Data.SqlClient;
using ParquetSharp;
using System.Data;
using System.Diagnostics.Metrics;
using System.Text;
using Encoding = System.Text.Encoding;

namespace EK4.NormalVersion
{
    public class EK4_ParquetOperation
    {
		public static string ek4filename = "";
		public static DateTime? today = null;
		public static DateTime? tomarrow = null;
		public async Task GetParquetFile()
        {

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
			//uat
			//builder.DataSource = "172.25.84.24";
			//builder.UserID = "quantra";
			//builder.Password = "quantra2";
			//builder.InitialCatalog = "EPara";
			//builder.Encrypt = true;
			//builder.TrustServerCertificate = true;
			//uat

			builder.DataSource = "PRD-SQL-ETUGRA1";
			builder.InitialCatalog = "EPara";
			builder.IntegratedSecurity = true;
			builder.TrustServerCertificate = true;
            builder.CommandTimeout = 0;



			// (EK4) E-para/Ödeme Kuruluşları Hesabı Kimlik/Bakiye/Kart Kimlik Formu
			// KK002_EPKBB_2020_12_20_0001.parquet
			//string sqlQuery = "EPara.dbo.GENERATE_PARQUET";


			string sqlQuery = "GENERATE_PARQUET_EK4";

            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {

                connection.Open();


                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {
					command.CommandType = CommandType.StoredProcedure;
					command.Parameters.AddWithValue("@TODAY", today);
					command.Parameters.AddWithValue("@TOMORROW", tomarrow);

					// Verileri SqlDataReader üzerinden oku
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
                        List<string> Uyruk = new List<string>();
                        List<string> Adresi = new List<string>();
                        List<string> IlceAdi = new List<string>();
                        List<string> PostaKodu = new List<string>();
                        List<string> IlKodu = new List<string>();
                        List<string> IlAdi = new List<string>();
                        List<string> Telefon = new List<string>();
                        List<string> Eposta = new List<string>();
                        List<string> HesapNo = new List<string>();
                        List<string> DovizTipi = new List<string>();
                        List<string> HesapTipi = new List<string>();
                        List<string> HesapDurumu = new List<string>();
                        List<string> HesapAcilisTarihi = new List<string>();
                        List<string> HesapKapanisTarihi = new List<string>();
                        List<string> HesapBakiyesi = new List<string>();
                        List<string> HesapBakiyesiTarihi = new List<string>();
                        List<string> KartDurumu = new List<string>();
                        List<string> KartAcilisTarihi = new List<string>();
                        List<string> KartKapanisTarihi = new List<string>();
                        List<string> KartNumarasi = new List<string>();
                        List<string> KurumKodu = new List<string>();



                        var columns = new Column[]
                        {
                            new Column<string>("recordtype"),
                            new Column<string>("lref"),
                            new Column<string>("islemturu"),
                            new Column<string>("hstkvkn"),
                            new Column<string>("hstkunvan"),
                            new Column<string>("hsgkad"),
                            new Column<string>("hsgksoyad"),
                            new Column<string>("hsgkkimliktipi"),
                            new Column<string>("hsgkkimlikno"),
                            new Column<string>("hsgkuyruk"),
                            new Column<string>("hsgkadres"),
                            new Column<string>("hsgkilceadi"),
                            new Column<string>("hsgkpostakod"),
                            new Column<string>("hsgkilkod"),
                            new Column<string>("hsgkiladi"),
                            new Column<string>("hstel"),
                            new Column<string>("hseposta"),
                            new Column<string>("hesno"),
                            new Column<string>("doviztip"),
                            new Column<string>("hsptip"),
                            new Column<string>("hspdurum"),
                            new Column<string>("hspaclstar"),
                            new Column<string>("hspkpnstar"),
                            new Column<string>("hspbakiye"),
                            new Column<string>("hspbakiyetarihi"),
                            new Column<string>("hspkartdurum"),
                            new Column<string>("hspkartaclstar"),
                            new Column<string>("hspkartkpnstar"),
                            new Column<string>("hspkartno"),
                            new Column<string>("kurumkod")
                        };
                        #region row_reader_area
                        while (await reader.ReadAsync())
                        {
                            RecordType.Add(reader.GetString("RecordType"));
                            LocalRef.Add(reader.GetInt64("Iref").ToString());
                            İslemTuruKodu.Add(reader.GetString("IslemTuruKodu"));
                            VergiKimlikNo.Add(reader.GetString("VergiKimlikNo"));
                            TuzelKisilikUnvan.Add(reader.GetString("TuzelKisilikUnvan"));
                            Ad.Add(reader.GetString("Ad"));
                            Soyad.Add(reader.GetString("Soyad"));
                            KimlikTipi.Add(reader.GetInt32("KimlikTipi").ToString());
                            KimlikNumarasi.Add(reader.GetString("KimlikNumarasi"));
                            Uyruk.Add(reader.GetString("Uyruk"));
                            Adresi.Add(reader.GetString("Adresi"));
                            IlceAdi.Add(reader.GetString("IlceAdi"));
                            PostaKodu.Add(reader.GetString("PostaKodu"));
                            IlKodu.Add(reader.GetString("IlKodu"));
                            IlAdi.Add(reader.GetString("IlAdi"));
                            Telefon.Add(reader.GetString("Telefon"));
                            Eposta.Add(reader.GetString("Eposta"));
                            HesapNo.Add(reader.GetString("HesapNo"));
                            DovizTipi.Add(reader.GetString("DovizTipi"));
                            HesapTipi.Add(reader.GetString("HesapTipi"));
                            HesapDurumu.Add(reader.GetString("HesapDurumu"));
                            HesapAcilisTarihi.Add(reader.GetDateTime("HesapAcilisTarihi").ToString());
                            HesapKapanisTarihi.Add(reader.GetString("HesapKapanisTarihi"));
                            HesapBakiyesi.Add(reader.GetDecimal("HesapBakiyesi").ToString());
                            HesapBakiyesiTarihi.Add(reader.GetDateTime("HesapBakiyeTarihi").ToString());
                            KartDurumu.Add(reader.GetInt32("KartDurumu").ToString());
                            KartAcilisTarihi.Add(reader.GetDateTime("KartAcilisTarihi").ToString());
                            KartKapanisTarihi.Add(reader.GetDateTime("KartKapanisTarihi").ToString());
                            KartNumarasi.Add(reader.GetString("KartNumarasi").ToString());
                            KurumKodu.Add(reader.GetString("KurumKodu"));

                        }
						#endregion

						ek4filename = GetDynamicFileName();

						using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ek4_output", ek4filename), columns);
                        using var rowGroup = file.AppendRowGroup();

                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(RecordType.ToArray());
                        }  //RecordType
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(LocalRef.ToArray());
                        }    //LocalRef
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(İslemTuruKodu.ToArray());
                        }  //İslemTuruKodu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(VergiKimlikNo.ToArray());
                        }  //VergiKimlikNo
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(TuzelKisilikUnvan.ToArray());
                        } //TuzelKisilikUnvan
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Ad.ToArray());
                        } //Ad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Soyad.ToArray());
                        } //Soyad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KimlikTipi.ToArray());
                        }    //KimlikTipi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KimlikNumarasi.ToArray());
                        } //KimlikNumarasi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Uyruk.ToArray());
                        } //Uyruk
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Adresi.ToArray());
                        } //Adresi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(IlceAdi.ToArray());
                        } //IlceAdi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(PostaKodu.ToArray());
                        } //PostaKodu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(IlKodu.ToArray());
                        } //IlKodu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(IlAdi.ToArray());
                        } //IlAdi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Telefon.ToArray());
                        } //Telefon
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(Eposta.ToArray());
                        } //Eposta
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(HesapNo.ToArray());
                        } //HesapNo
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(DovizTipi.ToArray());
                        } //DovizTipi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(HesapTipi.ToArray());
                        } //HesapTipi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(HesapDurumu.ToArray());
                        } //HesapDurumu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(HesapAcilisTarihi.ToArray());
                        }//HesapAcilisTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(HesapKapanisTarihi.ToArray());
                        } //HesapKapanisTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(HesapBakiyesi.ToArray());
                        }//HesapBakiyesi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(HesapBakiyesiTarihi.ToArray());
                        }//HesapBakiyesiTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KartDurumu.ToArray());
                        }    //KartDurumu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KartAcilisTarihi.ToArray());
                        }//KartAcilisTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KartKapanisTarihi.ToArray());
                        }//KartKapanisTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KartNumarasi.ToArray());
                        }//KartNumarasi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(KurumKodu.ToArray());
                        }//KurumKodu

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
			string prefix = "KK002_EPKBB_";
			string date = DateTime.Now.ToString("yyyy_MM_dd");
			string counterString = GetCounter();

			string fileName = $"{prefix}{date}_{counterString}.parquet";

			return fileName;
		}
	}
}

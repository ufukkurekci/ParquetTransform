using Microsoft.Data.SqlClient;
using ParquetSharp;
using System.Data;
using System.Text;
using Encoding = System.Text.Encoding;

namespace EK4.BinaryVersion
{
    public class EK4_ParquetOperation
    {
        public void GetParquetFile()
        {

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

            builder.DataSource = "172.25.84.24";
            builder.UserID = "quantra";
            builder.Password = "quantra2";
            builder.InitialCatalog = "EPara";
            builder.Encrypt = true;
            builder.TrustServerCertificate = true;


            // (EK4) E-para/Ödeme Kuruluşları Hesabı Kimlik/Bakiye/Kart Kimlik Formu
            // KK002_EPKBB_2020_12_20_0001.parquet
            string sqlQuery = "EPara.dbo.GENERATE_PARQUET";

            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {

                connection.Open();


                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {
                    // Verileri SqlDataReader üzerinden oku
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        List<byte> RecordType = new List<byte>();
                        List<byte> LocalRef = new List<byte>();
                        List<byte> İslemTuruKodu = new List<byte>();
                        List<byte> VergiKimlikNo = new List<byte>();
                        List<byte> TuzelKisilikUnvan = new List<byte>();
                        List<byte> Ad = new List<byte>();
                        List<byte> Soyad = new List<byte>();
                        List<byte> KimlikTipi = new List<byte>();
                        List<byte> KimlikNumarasi = new List<byte>();
                        List<byte> Uyruk = new List<byte>();
                        List<byte> Adresi = new List<byte>();
                        List<byte> IlceAdi = new List<byte>();
                        List<byte> PostaKodu = new List<byte>();
                        List<byte> IlKodu = new List<byte>();
                        List<byte> IlAdi = new List<byte>();
                        List<byte> Telefon = new List<byte>();
                        List<byte> Eposta = new List<byte>();
                        List<byte> HesapNo = new List<byte>();
                        List<byte> DovizTipi = new List<byte>();
                        List<byte> HesapTipi = new List<byte>();
                        List<byte> HesapDurumu = new List<byte>();
                        List<byte> HesapAcilisTarihi = new List<byte>();
                        List<byte> HesapKapanisTarihi = new List<byte>();
                        List<byte> HesapBakiyesi = new List<byte>();
                        List<byte> HesapBakiyesiTarihi = new List<byte>();
                        List<byte> KartDurumu = new List<byte>();
                        List<byte> KartAcilisTarihi = new List<byte>();
                        List<byte> KartKapanisTarihi = new List<byte>();
                        List<byte> KartNumarasi = new List<byte>();
                        List<byte> KurumKodu = new List<byte>();



                        var columns = new Column[]
                        {
                            new Column<byte>("recordtype"),
                            new Column<byte>("lref"),
                            new Column<byte>("islemturu"),
                            new Column<byte>("hstkvkn"),
                            new Column<byte>("hstkunvan"),
                            new Column<byte>("hsgkad"),
                            new Column<byte>("hsgksoyad"),
                            new Column<byte>("hsgkkimliktipi"),
                            new Column<byte>("hsgkkimlikno"),
                            new Column<byte>("hsgkuyruk"),
                            new Column<byte>("hsgkadres"),
                            new Column<byte>("hsgkilceadi"),
                            new Column<byte>("hsgkpostakod"),
                            new Column<byte>("hsgkilkod"),
                            new Column<byte>("hsgkiladi"),
                            new Column<byte>("hstel"),
                            new Column<byte>("hseposta"),
                            new Column<byte>("hesno"),
                            new Column<byte>("doviztip"),
                            new Column<byte>("hsptip"),
                            new Column<byte>("hspdurum"),
                            new Column<byte>("hspaclstar"),
                            new Column<byte>("hspkpnstar"),
                            new Column<byte>("hspbakiye"),
                            new Column<byte>("hspbakiyetarihi"),
                            new Column<byte>("hspkartdurum"),
                            new Column<byte>("hspkartaclstar"),
                            new Column<byte>("hspkartkpnstar"),
                            new Column<byte>("hspkartno"),
                            new Column<byte>("kurumkod")
                        };
                        #region row_reader_area
                        while (reader.Read())
                        {
                            RecordType.AddRange(Encoding.UTF8.GetBytes(reader.GetString("RecordType")));
                            LocalRef.AddRange(Encoding.UTF8.GetBytes(reader.GetInt64("Iref").ToString()));
                            İslemTuruKodu.AddRange(Encoding.UTF8.GetBytes(reader.GetString("IslemTuruKodu")));
                            VergiKimlikNo.AddRange(Encoding.UTF8.GetBytes(reader.GetString("VergiKimlikNo")));
                            TuzelKisilikUnvan.AddRange(Encoding.UTF8.GetBytes(reader.GetString("TuzelKisilikUnvan")));
                            Ad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("Ad")));
                            Soyad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("Soyad")));
                            KimlikTipi.AddRange(Encoding.UTF8.GetBytes(reader.GetInt32("KimlikTipi").ToString()));
                            KimlikNumarasi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("Uyruk")));
                            Adresi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("Adresi")));
                            IlceAdi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("IlceAdi")));
                            PostaKodu.AddRange(Encoding.UTF8.GetBytes(reader.GetString("PostaKodu")));
                            IlKodu.AddRange(Encoding.UTF8.GetBytes(reader.GetString("IlKodu")));
                            IlAdi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("IlAdi")));
                            Telefon.AddRange(Encoding.UTF8.GetBytes(reader.GetString("Telefon")));
                            Eposta.AddRange(Encoding.UTF8.GetBytes(reader.GetString("Eposta")));
                            HesapNo.AddRange(Encoding.UTF8.GetBytes(reader.GetString("HesapNo")));
                            DovizTipi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("DovizTipi")));
                            HesapTipi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("HesapTipi")));
                            HesapDurumu.AddRange(Encoding.UTF8.GetBytes(reader.GetString("HesapDurumu")));
                            HesapAcilisTarihi.AddRange(Encoding.UTF8.GetBytes(reader.GetDateTime("HesapAcilisTarihi").ToString()));
                            HesapKapanisTarihi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("HesapKapanisTarihi")));
                            HesapBakiyesi.AddRange(Encoding.UTF8.GetBytes(reader.GetDecimal("HesapBakiyesi").ToString()));
                            HesapBakiyesiTarihi.AddRange(Encoding.UTF8.GetBytes(reader.GetDateTime("HesapBakiyeTarihi").ToString()));
                            KartDurumu.AddRange(Encoding.UTF8.GetBytes(reader.GetInt32("KartDurumu").ToString()));
                            KartAcilisTarihi.AddRange(Encoding.UTF8.GetBytes(reader.GetDateTime("KartAcilisTarihi").ToString()));
                            KartKapanisTarihi.AddRange(Encoding.UTF8.GetBytes(reader.GetDateTime("KartKapanisTarihi").ToString()));
                            KartNumarasi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("KartNumarasi").ToString()));
                            KurumKodu.AddRange(Encoding.UTF8.GetBytes(reader.GetString("KurumKodu")));

                        }
                        #endregion

                        using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "parquet", "KK002_EPKBB_2020_12_20_0001.parquet"), columns);
                        using var rowGroup = file.AppendRowGroup();

                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(RecordType.ToArray());
                        }  //RecordType
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(LocalRef.ToArray());
                        }    //LocalRef
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(İslemTuruKodu.ToArray());
                        }  //İslemTuruKodu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(VergiKimlikNo.ToArray());
                        }  //VergiKimlikNo
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(TuzelKisilikUnvan.ToArray());
                        } //TuzelKisilikUnvan
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(Ad.ToArray());
                        } //Ad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(Soyad.ToArray());
                        } //Soyad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(KimlikTipi.ToArray());
                        }    //KimlikTipi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(KimlikNumarasi.ToArray());
                        } //KimlikNumarasi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(Uyruk.ToArray());
                        } //Uyruk
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(Adresi.ToArray());
                        } //Adresi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(IlceAdi.ToArray());
                        } //IlceAdi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(PostaKodu.ToArray());
                        } //PostaKodu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(IlKodu.ToArray());
                        } //IlKodu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(IlAdi.ToArray());
                        } //IlAdi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(Telefon.ToArray());
                        } //Telefon
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(Eposta.ToArray());
                        } //Eposta
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(HesapNo.ToArray());
                        } //HesapNo
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(DovizTipi.ToArray());
                        } //DovizTipi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(HesapTipi.ToArray());
                        } //HesapTipi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(HesapDurumu.ToArray());
                        } //HesapDurumu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(HesapAcilisTarihi.ToArray());
                        }//HesapAcilisTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(HesapKapanisTarihi.ToArray());
                        } //HesapKapanisTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(HesapBakiyesi.ToArray());
                        }//HesapBakiyesi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(HesapBakiyesiTarihi.ToArray());
                        }//HesapBakiyesiTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(KartDurumu.ToArray());
                        }    //KartDurumu
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(KartAcilisTarihi.ToArray());
                        }//KartAcilisTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(KartKapanisTarihi.ToArray());
                        }//KartKapanisTarihi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(KartNumarasi.ToArray());
                        }//KartNumarasi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(KurumKodu.ToArray());
                        }//KurumKodu

                        file.Close();

                    }
                }
            }
        }
    }
}

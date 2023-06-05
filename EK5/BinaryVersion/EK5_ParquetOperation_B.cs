using Microsoft.Data.SqlClient;
using ParquetSharp;
using System.Data;
using System.Text;
using Encoding = System.Text.Encoding;

namespace EK5.BinaryVersion
{
    public class EK5_ParquetOperation_B
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


            // (EK5) E-para/Ödeme Kuruluşu Hesabına Para Yükleme/Çekme(Nakit) Formu
            // KK002_EPHPYCNI_2020_12_20_0001.parquet
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
                        List<byte> musterimi = new List<byte>();
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
                        List<byte> kisiad = new List<byte>();
                        List<byte> kisisoyad = new List<byte>();
                        List<byte> kisikimliktipi = new List<byte>();
                        List<byte> kisikimlikno = new List<byte>();
                        List<byte> istar = new List<byte>();
                        List<byte> isknl = new List<byte>();
                        List<byte> bankaad = new List<byte>();
                        List<byte> islemtutar = new List<byte>();
                        List<byte> asilparatutar = new List<byte>();
                        List<byte> parabirim = new List<byte>();
                        List<byte> brutkomtut = new List<byte>();
                        List<byte> kuraciklama = new List<byte>();
                        List<byte> musaciklama = new List<byte>();
                        List<byte> KurumKod = new List<byte>();



                        var columns = new Column[]
                        {
                            new Column<byte>("recordtype"),
                            new Column<byte>("lref"),
                            new Column<byte>("islemturu"),
                            new Column<byte>("musterimi"),
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
                            new Column<byte>("kisiad"),
                            new Column<byte>("kisisoyad"),
                            new Column<byte>("kisikimliktipi"),
                            new Column<byte>("kisikimlikno"),
                            new Column<byte>("istar"),
                            new Column<byte>("isknl"),
                            new Column<byte>("bankaad"),
                            new Column<byte>("islemtutar"),
                            new Column<byte>("asilparatutar"),
                            new Column<byte>("parabirim"),
                            new Column<byte>("brutkomtut"),
                            new Column<byte>("musaciklama"),
                            new Column<byte>("kuraciklama"),
                            new Column<byte>("kurumkod")
                        };
                        #region row_reader_area
                        while (reader.Read())
                        {
                            RecordType.AddRange(Encoding.UTF8.GetBytes(reader.GetString("RecordType")));
                            LocalRef.AddRange(Encoding.UTF8.GetBytes(reader.GetString("Iref")));
                            İslemTuruKodu.AddRange(Encoding.UTF8.GetBytes(reader.GetString("IslemTuruKodu")));
                            musterimi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("musterimi")));
                            VergiKimlikNo.AddRange(Encoding.UTF8.GetBytes(reader.GetString("VergiKimlikNo")));
                            TuzelKisilikUnvan.AddRange(Encoding.UTF8.GetBytes(reader.GetString("TuzelKisilikUnvan")));
                            Ad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("Ad")));
                            Soyad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("Soyad")));
                            KimlikTipi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("KimlikTipi")));
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
                            kisiad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("kisiad")));
                            kisisoyad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("kisisoyad")));
                            kisikimliktipi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("kisikimliktipi")));
                            kisikimlikno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("kisikimlikno")));
                            istar.AddRange(Encoding.UTF8.GetBytes(reader.GetString("istar")));
                            isknl.AddRange(Encoding.UTF8.GetBytes(reader.GetString("isknl")));
                            bankaad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("bankaad")));
                            islemtutar.AddRange(Encoding.UTF8.GetBytes(reader.GetString("islemtutar")));
                            asilparatutar.AddRange(Encoding.UTF8.GetBytes(reader.GetString("asilparatutar")));
                            parabirim.AddRange(Encoding.UTF8.GetBytes(reader.GetString("parabirim")));
                            brutkomtut.AddRange(Encoding.UTF8.GetBytes(reader.GetString("brutkomtut")));
                            musaciklama.AddRange(Encoding.UTF8.GetBytes(reader.GetString("musaciklama")));
                            kuraciklama.AddRange(Encoding.UTF8.GetBytes(reader.GetString("kuraciklama")));
                            KurumKod.AddRange(Encoding.UTF8.GetBytes(reader.GetString("kurumkod")));

                        }
                        #endregion

                        using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "parquet", "KK002_EPHPYCNI_2020_12_20_0001.parquet"), columns);
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
                            objectIdWriter.WriteBatch(musterimi.ToArray());
                        }  //musterimi
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
                            objectIdWriter.WriteBatch(kisiad.ToArray());
                        } //kisiad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(kisisoyad.ToArray());
                        } //kisisoyad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(kisikimliktipi.ToArray());
                        } //kisikimliktipi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(kisikimlikno.ToArray());
                        }//kisikimlikno
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(istar.ToArray());
                        }//istar
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(isknl.ToArray());
                        }//isknl 
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(bankaad.ToArray());
                        }//bankaad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(islemtutar.ToArray());
                        }//islemtutar
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(asilparatutar.ToArray());
                        }//asilparatutar
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(parabirim.ToArray());
                        }//parabirim
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(brutkomtut.ToArray());
                        }//brutkomtut
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(musaciklama.ToArray());
                        }//musaciklama
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(kuraciklama.ToArray());
                        }//kuraciklama
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(KurumKod.ToArray());
                        }//kurumkod

                        file.Close();

                    }
                }
            }
        }
    }
}

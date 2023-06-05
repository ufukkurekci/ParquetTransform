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


			string sqlQuery = "DECLARE @TODAY DATETIME = '20230602', @TOMORROW DATETIME = '20230603'\n\nSELECT  distinct \n'E' as RecordType,\ncam.[RECORD_ID] Iref,\n'OK003' as IslemTuruKodu,\ncase\nwhen customer.TaxNo is null or len(customer.TaxNo) =0 then '9999999999'\nelse customer.TaxNo\nend as VergiKimlikNo,\ncase\nwhen customer.FirmName is null or len(customer.FirmName) =0 then 'XXXXXXXXXX'\nelse customer.FirmName\nend as TuzelKisilikUnvan,\ncase\nwhen (customer.Name) is null or len(customer.Name) =0 and customer.Name is null then 'XXXXXXXXXX'\nelse UPPER(customer.Name)\nend as Ad,\ncase\nwhen (customer.Surname) is null or len(customer.Surname) =0 and customer.FirmName is null then 'XXXXXXXXXX'\nelse UPPER(customer.Surname)\nend as Soyad,\ncase when customer.IdentityType = 1 THEN 4\n     when customer.IdentityType = 2 THEN 5\n     when customer.IdentityType = 3 THEN 3\n     when customer.IdentityType = 4 THEN 1\n     else 5\nEND as KimlikTipi,\ncase\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or  len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo))= 0 then '00000000'\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo) end  as KimlikNumarasi,\ncase\nwhen n.ISOCode is not null then n.ISOCode \nelse '00'\nend as Uyruk,\ncase\nwhen customer.CustomerType = 1 and (UPPER(ca.District+' '+ca.Street+' '+ca.AddressDetail+' '+ca.Town+' / '+ca.City)) is not null then  UPPER(ca.District+' '+ca.Street+' '+ca.AddressDetail+' '+ca.Town+' / '+ca.City)\nwhen customer.CustomerType = 2 then '' --tüzellerin adresleri nereden alınacak?\nelse 'XXXXXXXXXX'\nend as Adresi,\ncase\nwhen UPPER(ca.Town)is not null then UPPER(ca.Town)\nelse 'XXXXXXXXXX'\nend as IlceAdi,\ncase\nwhen UPPER(ca.PostalCode)is not null then UPPER(ca.PostalCode)\nelse 'XXXXXXXXXX'\nend as PostaKodu,\ncase\nwhen UPPER(ca.CityCode)is not null then UPPER(ca.CityCode)\nelse 'XXXXXXXXXX'\nend as IlKodu,\ncase\nwhen UPPER(ca.City)is not null then UPPER(ca.City)\nelse 'XXXXXXXXXX'\nend as IlAdi,\ncase\nwhen (SELECT TOP (1) CompleteTelephoneNumber FROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon with (nolock) WHERE telefon.Id = customer.Id and telefon.TelephoneType = 1 and telefon.RecordStatus ='A' ORDER BY telefon.RecordTime DESC) is null then '999999999999'\nELSE (SELECT TOP (1) CompleteTelephoneNumber FROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon with (nolock) WHERE telefon.Id = customer.Id and telefon.TelephoneType = 1 and telefon.RecordStatus ='A' ORDER BY telefon.RecordTime DESC)\nEND as Telefon,\n\ncase\nwhen (SELECT TOP (1) Email FROM [CustomerDb].[dbo].[CustomerEmail] AS email with (nolock) WHERE email.Id = customer.Id and email.EmailType = 1 and email.RecordStatus ='A' ORDER BY email.RecordTime DESC) is null then 'XXXX@XXX.COM'\nELSE (SELECT TOP (1) Email FROM [CustomerDb].[dbo].[CustomerEmail] AS email with (nolock) WHERE email.Id = customer.Id and email.EmailType = 1 and email.RecordStatus ='A' ORDER BY email.RecordTime DESC)\nEND as Eposta,\n\n'999999999' as HesapNo,\n'XXX' as DovizTipi,\n'X' as HesapTipi,\n'X' as HesapDurumu,\ncustomer.EntryDate as HesapAcilisTarihi,\n'YYYYMMDD' as HesapKapanisTarihi,\n\n\ncase\nwhen (SELECT TOP (1) CARD_BALANCE_NEW  FROM EPara.TRN.CARD_TRN_MASTER AS Bakiye with (nolock) WHERE Bakiye.MIFARE_ID = cm.MIFARE_ID and TRN_STATUS IN (1,5) ORDER BY Bakiye.TRN_DATE  DESC) is null then 0\nELSE (SELECT TOP (1) CARD_BALANCE_NEW   FROM EPara.TRN.CARD_TRN_MASTER AS Bakiye with (nolock)  WHERE Bakiye.MIFARE_ID = cm.MIFARE_ID and TRN_STATUS IN (1,5) ORDER BY Bakiye.TRN_DATE  DESC)\n\nEND as HesapBakiyesi,\ncase\nwhen (SELECT TOP (1) INSERT_DATE   FROM EPara.TRN.CARD_TRN_MASTER AS Bakiye with (nolock) WHERE Bakiye.MIFARE_ID = cm.MIFARE_ID and TRN_STATUS IN (1,5) ORDER BY Bakiye.TRN_DATE  DESC) is null then 0\nELSE (SELECT TOP (1) INSERT_DATE   FROM EPara.TRN.CARD_TRN_MASTER AS Bakiye with (nolock) WHERE Bakiye.MIFARE_ID = cm.MIFARE_ID and TRN_STATUS IN (1,5)  ORDER BY Bakiye.TRN_DATE  DESC)\n\nEND as HesapBakiyeTarihi,\n\nCASE\nwhen s.CARD_STATUS_DESCRIPTION='Açık'  THEN 1\nwhen s.CARD_STATUS_DESCRIPTION='Basım Blokeli'  THEN 4\nwhen s.CARD_STATUS_DESCRIPTION='Bayi Kese Kart Kapatma' THEN 4\nwhen s.CARD_STATUS_DESCRIPTION='Çalıntı' THEN 4\nwhen s.CARD_STATUS_DESCRIPTION='Geçici Blokeli' THEN 4\nwhen s.CARD_STATUS_DESCRIPTION='İptal' THEN 3\nwhen s.CARD_STATUS_DESCRIPTION='Kayıp' THEN 4\nwhen s.CARD_STATUS_DESCRIPTION='Müşteri İsteği ile İptal' THEN 3\nwhen s.CARD_STATUS_DESCRIPTION='Normal' THEN 1\nwhen s.CARD_STATUS_DESCRIPTION='Sahte Kart' THEN 4\nwhen s.CARD_STATUS_DESCRIPTION='Vadesi Dolmuş Kart' THEN 4\nwhen s.CARD_STATUS_DESCRIPTION='Yenileme' THEN 1\nelse 4\nEND as KartDurumu,\ncase\nwhen COALESCE(cam.INSERT_DATE,cm.INSERT_DATE)  is not null then cast(COALESCE(cam.INSERT_DATE,cm.INSERT_DATE)  as nvarchar)\nelse '00000000 000000'\nend as KartAcilisTarihi,\ncase\nwhen cm.CARD_CLOSE_DATE  is not null then cast(cm.CARD_CLOSE_DATE as nvarchar)\nelse '00000000 000000'\nend as KartKapanisTarihi,\ncase\nwhen cm.MIFARE_ID is not null then cm.MIFARE_ID\nelse 'xxxxxxxxxxx'\nend as KartNumarasi,\n'KK002' AS KurumKodu\nFROM CustomerDb.dbo.Customers customer with (nolock)\nLEFT JOIN [CustomerDb].[dbo].[CustomerIndividuals] as cı with (nolock) on cı.Id = customer.Id\nLEFT OUTER JOIN [CustomerDb].[dbo].[CustomerAddressNew] AS ca WITH (NOLOCK) ON customer.[Id] =ca.Id and ca.RecordStatus='A' AND ca.AddressType=7\nLEFT JOIN [EPara].[CRD].[CARD_MASTER] as cm with (nolock) on cm.CUSTOMER_NUMBER =customer.Id\nLEFT JOIN [EPara].[PRM].[CARD_STATUS] AS s WITH (NOLOCK) ON cm.CARD_STATUS_CODE=s.CARD_STATUS_CODE\nLEFT JOIN [EPara].[CRD].[CARD_APPLICATION_MASTER] AS cam WITH (NOLOCK) ON cm.[CARD_APPLICATION_MASTER_ID]=cam.[RECORD_ID]\nLEFT JOIN [CustomerDb].[dbo].[PCountryCode_TBL] AS n WITH (NOLOCK) ON n.CountryCode = cı.Nationality AND n.TENANT_CODE='BELBIM'\nWHERE customer.RecordStatus ='A'  and (cast(cam.INSERT_DATE as date) >= @TODAY and (cast(cam.INSERT_DATE as date) < @TOMORROW))";

            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {

                connection.Open();


                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {
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

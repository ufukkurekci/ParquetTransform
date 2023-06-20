using GlobalHelper;
using Microsoft.Data.SqlClient;
using ParquetSharp;
using System.Data;
using System.Text;

namespace EK7.NormalVersion
{
	public class EK7_ParquetOpertion
    {
        public static string ek7filename = "";
        public static string? today = null;
        public static string? tomarrow = null;

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

            string sqlQuery = $"DECLARE @TODAY DATETIME = '{today}', @TOMORROW DATETIME = '{tomarrow}'\r\nSELECT distinct 'E'                                                                    as recordtype,\r\n                ctm.RECORD_ID                                                          as lref,\r\n                'KK002'                                                                as islemturu,\r\n                '9999999999'                                                           as ksahtkvkn,\r\n                'XXXXXXXXXX'                                                           as ksahtkunvan,\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\n                    end                                                                as ksahgkad,\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Surname)\r\n                    end                                                                as ksahgksoyad,\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\n                    END                                                                as ksahgkkimliktipi,\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or  len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo))= 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo) end as ksahgkkimlikno,\r\n                case\r\n                    when (ctm.MIFARE_ID) is null or len(ctm.MIFARE_ID) = 0\r\n                        then '0000000000'\r\n                    else ctm.MIFARE_ID\r\n                    end                                                                as ksahkartno,\r\n                '1'                                                                    as banktip,\r\n                '7777'                                                                 as bankeftkod,\r\n                '777777777777777'                                                      as bankatmkod,\r\n                case\r\n                    when (ctm.TRN_DATE) is null or len(ctm.TRN_DATE) = 0\r\n                        then '00000000'\r\n\r\n                    else ctm.TRN_DATE\r\n                    end                                                                as istar,\r\n                case\r\n                    when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                        then '0'\r\n                    else ctm.TRN_AMOUNT\r\n                    end                                                                as islemtutar,\r\n                case\r\n                    when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                        then '0'\r\n                    else ctm.TRN_AMOUNT\r\n                    end                                                                as asiltutar,\r\n                '949'                                                                  as parabirim,\r\n                '0'                                                                    as brutkomtut,\r\n                'XXXXXXXXX'                                                            as musaciklama,\r\n                case\r\n                    when (tcm.TRN_CODE_DESCRIPTION) is null or len(tcm.TRN_CODE_DESCRIPTION) = 0\r\n                        then 'XXXXXXXXX'\r\n                    else tcm.TRN_CODE_DESCRIPTION\r\n                    end                                                                as kuraciklama,\r\n                '002'                                                                  as kurumkod\r\nFROM EPara.CRD.CARD_MASTER as cm with (NOLOCK)\r\n         INNER JOIN [EPara].[TRN].[CARD_TRN_MASTER] ctm\r\n    WITH (nolock)\r\n                    ON ctm.MIFARE_ID = cm.MIFARE_ID and COALESCE(ctm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPARA.TRN.CARD_TRN_LOAD ctl\r\n    WITH (NOLOCK)\r\n                   ON ctm.RECORD_ID = ctl.TRN_MASTER_ID and COALESCE(ctl.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPara.PRM.TRN_CODE_MATRIX AS tcm\r\n    WITH (nolock)\r\n                   ON tcm.TRN_CODE = ctm.TRN_CODE and COALESCE(tcm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPara.POS.TERMINAL_MASTER as tm\r\n    with (nolock)\r\n                   on ctm.TERMINAL_NUMBER = tm.TERMINAL_NUMBER\r\n         LEFT JOIN [EPara].[PRM].[TERMINAL_TYPE] as tt\r\n    WITH (NOLOCK)\r\n                   ON tm.TERMINAL_TYPE = tt.TERMINAL_TYPE_CODE\r\n         LEFT JOIN CustomerDb.dbo.Customers customer on customer.Id = cm.CUSTOMER_NUMBER\r\nWHERE COALESCE(cm.RECORD_STATUS\r\n          , 'A') = 'A'\r\n  AND (\r\n        (\r\n                    ctm.TRN_STATUS in (1 /*'BAŞARILI'*/\r\n                    , 5 /*'ASKIDA BAŞARILI'*/\r\n                    )\r\n                AND ctm.TRN_CODE IN (\r\n                /*Harcama*/\r\n                                     '01080011000' --Harcama Kontörlü - Ulaşım\r\n                , '51080011000' --Harcama İade Kontörlü - Ulaşım\r\n                , '01080061000' --Harcama Kontörlü QR - Ulaşım\r\n                , '51080061000' /*Harcama İade Kontörlü QR - Ulaşım*/\r\n                , '05020010003' --Vizeleme Görevi - Ücretli - Biletmatik\r\n                , '05020010305' --Vizeleme Görevi - Kurumsal - Biletmatik\r\n                , '05020010006' --Vizeleme Görevi - Kart Bedelli - Biletmatik\r\n                , '05050010003' --Vizeleme Görevi - Ücretli - Akdom\r\n                , '05050010305' --Vizeleme Görevi - Kurumsal - Akdom\r\n                , '05050010006' --Vizeleme Görevi - Kart Bedelli - Akdom\r\n\r\n                )\r\n            )\r\n        OR (ctm.TRN_STATUS IN (1)\r\n        and ctl.TRN_RESPONSE = 1\r\n        and\r\n            ctm.TRN_CODE IN ('01010010000')) --/*Harcama - POS - Sanal Kese*/\r\n        OR (ctm.TRN_CODE = '51010010000'\r\n        and ctm.TRN_STATUS IN (1, 5)) --Harcama İade - POS - Sanal Kese\r\n    )\r\n and (cast(ctm.TRN_DATE as date) >= @TODAY and (cast(ctm.TRN_DATE as date) < @TOMORROW))";

			using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {

                connection.Open();


                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {

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

                        ek7filename = FileNameHelper.GetDynamicFileName("KK002_OKKIB_");

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

						string localFilePath = SftpHelper.LocalFilePath(ek7filename, "ek3_output");

                        string remoteDirectory = "/data/real/OKKIB/";

						SftpHelper.ConnectSftp(localFilePath, remoteDirectory);
					}
                }
            }
        }
	}
}

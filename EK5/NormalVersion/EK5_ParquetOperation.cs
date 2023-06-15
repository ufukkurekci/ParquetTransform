using GlobalHelper;
using Microsoft.Data.SqlClient;
using ParquetSharp;
using System.Data;
using System.Text;
using Encoding = System.Text.Encoding;

namespace EK5.NormalVersion
{
    public class EK5_ParquetOperation
    {
		public static string ek5filename = "";
		public static string? today = null;
		public static string? tomarrow = null;
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
            //builder.CommandTimeout = 0;
            //uat

            builder.DataSource = "PRD-SQL-ETUGRA1";
            builder.InitialCatalog = "EPara";
            builder.IntegratedSecurity = true;
            builder.TrustServerCertificate = true;
            builder.CommandTimeout = 0;

            // (EK5) E-para/Ödeme Kuruluşu Hesabına Para Yükleme/Çekme(Nakit) Formu
            // KK002_EPHPYCNI_2020_12_20_0001.parquet
            string sqlQuery = $"DECLARE @TODAY DATETIME = '{today}', @TOMORROW DATETIME = '{tomarrow}'\r\n\r\nSELECT distinct  'E'                                                                    as recordtype,\r\n                ctl.RECORD_ID                                                          as lref,\r\n                'OH011'                                                                as islemturu,\r\n                'E'                                                                    as musterimi,\r\n                case\r\n                    when customer.TaxNo is null or len(customer.TaxNo) = 0 then '9999999999'\r\n                    else customer.TaxNo\r\n                    end                                                                as hestkvkn,\r\n                case\r\n                    when customer.FirmName is null or len(customer.FirmName) = 0 then 'XXXXXXXXXX'\r\n                    else customer.FirmName\r\n                    end                                                                as hestkunvan,\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\n                    end                                                                as hesgkad,\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Surname)\r\n                    end                                                                as hesgksoyad,\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\n                    END                                                                as hesgkkimliktipi,\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or\r\n                         len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo)) = 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo) end as hesgkkimlikno,\r\n                case\r\n                    when n.ISOCode is null or len(n.ISOCode) = 0 then 'XX'\r\n                    else n.ISOCode\r\n                    end                                                                as hesgkuyruk,\r\n                'XXX'                                                                  as hesgkadres,\r\n                'XXX'                                                                  as hesgkilceadi,\r\n                'XXX'                                                                  as hesgkpostakod,\r\n                case\r\n                    when UPPER(ca.CityCode) is not null then CONCAT('0', UPPER(ca.CityCode))\r\n                    else '000'\r\n                    end                                                                as hesgkilkod,\r\n                'XXX'                                                                  as hesgkiladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone]                  AS telefon with (nolock) WHERE telefon.Id = customer.Id\r\n                            and telefon.TelephoneType = 1\r\n                            and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon with (nolock)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nEND\r\nas hestel,\r\n                'XXX'                                                                  as heseposta,\r\n                case when ctm.MIFARE_ID is null or len(ctm.MIFARE_ID)= 0 then '00000' else ctm.MIFARE_ID\r\nend    as hesno,\r\n                'TRY'                                                                  as doviztip,\r\n                '9'                                                                    as hsptip,\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\nend\r\nas kisiad,\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Surname)\r\nend\r\nas kisisoyad,\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\nEND\r\nas kisikimliktipi,\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or  len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo))= 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo)\r\nend as kisikimlikno,\r\n                case\r\n                    when cast(ctm.TRN_DATE as date) is null then '00000000'\r\n                    else cast(ctm.TRN_DATE as date)\r\nend                                as istar,\r\n                'XXX'                                                                  as isknl,\r\n                'XXX'                                                                  as bankaad,\r\n                case\r\n                    when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0 then 0\r\n                    else\r\n                        ctm.TRN_AMOUNT\r\nend\r\nas islemtutar,\r\n                case\r\n                    when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0 then 0\r\n                    else\r\n                        ctm.TRN_AMOUNT\r\nend\r\nas asilparatutar,\r\n                '949'                                                                  as parabirim,\r\n                '0'                                                                    as brutkomtut,\r\n                'XXXXX'                                                                as musaciklama,\r\n                case\r\n                    when (tcm.TRN_CODE_DESCRIPTION) is null or len(tcm.TRN_CODE_DESCRIPTION) = 0 then 'XXXXXXXXX'\r\n                    else\r\n                        tcm.TRN_CODE_DESCRIPTION\r\nend\r\nas kuraciklama,\r\n                '002'                                                                  as kurumkod\r\nFROM EPara.CRD.CARD_MASTER as cm\r\n         with (NOLOCK)\r\n         INNER JOIN [EPara].[TRN].[CARD_TRN_MASTER] ctm\r\n    WITH (nolock) ON ctm.MIFARE_ID = cm.MIFARE_ID and COALESCE(ctm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPARA.TRN.CARD_TRN_LOAD ctl\r\n    WITH (NOLOCK) ON ctm.RECORD_ID = ctl.TRN_MASTER_ID and COALESCE(ctl.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPara.PRM.TRN_CODE_MATRIX AS tcm\r\n    WITH (nolock) ON tcm.TRN_CODE = ctm.TRN_CODE and COALESCE(tcm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPara.POS.TERMINAL_MASTER as tm\r\n    with (nolock) on ctm.TERMINAL_NUMBER = tm.TERMINAL_NUMBER\r\n         LEFT JOIN [EPara].[PRM].[TERMINAL_TYPE] as tt\r\n    WITH (NOLOCK) ON tm.TERMINAL_TYPE = tt.TERMINAL_TYPE_CODE\r\n         left join CustomerDb.dbo.Customers customer\r\n    WITH (NOLOCK) on cm.CUSTOMER_NUMBER = customer.Id\r\n         LEFT OUTER JOIN [CustomerDb].[dbo].[CustomerAddressNew] AS ca\r\n    WITH (NOLOCK) ON customer.[Id] = ca.Id and ca.RecordStatus = 'A' AND ca.AddressType = 7\r\n\r\n         LEFT JOIN [CustomerDb].[dbo].[CustomerIndividuals] as ci\r\n    WITH (NOLOCK) on ci.Id = customer.Id\r\n         LEFT JOIN [CustomerDb].[dbo].[PCountryCode_TBL] AS n\r\n    WITH (NOLOCK) ON n.CountryCode = ci.Nationality AND n.TENANT_CODE = 'BELBIM'\r\n\r\nWHERE COALESCE(cm.RECORD_STATUS\r\n          , 'A') = 'A'\r\nand (cast(ctm.INSERT_DATE as date) >= @TODAY and (cast(ctm.INSERT_DATE as date) < @TOMORROW))\r\n  and  (cast(ctm.TRN_DATE as date) >= @TODAY and (cast(ctm.TRN_DATE as date) < @TOMORROW))\r\n\r\n-- Akdom- Pos- Bilatmatik yukleme ve iadeleri\r\n  and (\r\n        (ctm.TRN_STATUS in (1, 5)\r\n            and ctm.TRN_CODE IN\r\n                ('02010011000', '02020011100', '02020011200', '02050011000', '52010011000'/*TL IADE*/, '52050011000'/*TL IADE*/,\r\n                 '07060111000'/* IBM BAKIYE AKTARIM*/\r\n                    ))\r\n        OR (ctm.TRN_STATUS = 4\r\n        AND ctl.TRN_RESPONSE = 1\r\n        AND ctm.TRN_CODE IN ('02010011000'/*TL POS*/, '52010011000'/*TL POS IADE*/))\r\n    )\r\n  and ctl.RECORD_ID is not null\r\n\r\nUNION ALL\r\n\r\n--Mobile ve Web yuklemeleri\r\nSELECT distinct  'E'                                                                    as recordtype,\r\n                v.RECORD_ID                                                            as lref,\r\n                'OH011'                                                                as islemturu,\r\n                'E'                                                                    as musterimi,\r\n                case\r\n                    when customer.TaxNo is null or len(customer.TaxNo) = 0 then '9999999999'\r\n                    else customer.TaxNo\r\n                    end                                                                as hestkvkn,\r\n                case\r\n                    when customer.FirmName is null or len(customer.FirmName) = 0 then 'XXXXXXXXXX'\r\n                    else customer.FirmName\r\n                    end                                                                as hestkunvan,\r\n\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\n                    end                                                                as hesgkad,\r\n\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n\r\n                    else UPPER(customer.Surname)\r\n                    end                                                                as hesgksoyad,\r\n\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\n                    END                                                                as hesgkkimliktipi,\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or\r\n                         len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo)) = 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo) end as hesgkkimlikno,\r\n                case\r\n                    when (n.ISOCode) is null or len(n.ISOCode) = 0 then 'XX'\r\n                    else\r\n                        n.ISOCode\r\n                    end                                                                as hesgkuyruk,\r\n                'XXX'                                                                  as hesgkadres,\r\n                'XXX'                                                                  as hesgkilceadi,\r\n                'XXX'                                                                  as hesgkpostakod,\r\n                case\r\n                    when UPPER(ca.CityCode) is not null then CONCAT('0', UPPER(ca.CityCode))\r\n                    else '000'\r\n                    end                                                                as hesgkilkod,\r\n                'XXX'                                                                  as hesgkiladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone]                  AS telefon WITH (NOLOCK) WHERE telefon.Id = customer.Id\r\n                            and telefon.TelephoneType = 1\r\n                            and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nEND\r\nas hestel,\r\n                'XXX'                                                                  as heseposta,\r\n                case when v.MIFARE_ID is null or len(v.MIFARE_ID) =0 then '00000' else v.MIFARE_ID\r\nend        as hesno,\r\n                'TRY'                                                                  as doviztip,\r\n                '9'                                                                    as hsptip,\r\n\r\n\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\nend\r\nas kisiad,\r\n\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n\r\n                    else UPPER(customer.Surname)\r\nend\r\nas kisisoyad,\r\n\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\nEND\r\nas kisikimliktipi,\r\n\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or  len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo))= 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo)\r\nend as kisikimlikno,\r\n\r\n                case\r\n                    when cast(v.INSERT_DATE as date) is null then '00000000'\r\n                    else cast(v.INSERT_DATE as date)\r\nend                               as istar\r\n        ,\r\n\r\n                'XXX'                                                                  as isknl,\r\n                'XXX'                                                                  as bankaad,\r\n                case\r\n                    when (v.[AMOUNT]) is null or len(v.[AMOUNT]) = 0 then 0\r\n                    else\r\n                        v.[AMOUNT]\r\nend\r\nas islemtutar,\r\n                case\r\n                    when (v.[AMOUNT]) is null or len(v.[AMOUNT]) = 0 then 0\r\n                    else\r\n                        v.[AMOUNT]\r\nend\r\nas asilparatutar,\r\n                '949'                                                                  as parabirim,\r\n                '0'                                                                    as brutkomtut,\r\n                'XXXXX'                                                                as musaciklama,\r\n                case\r\n                    when REF_TRN_TYPE = 1 then 'Kart Basvuru'\r\n                    when REF_TRN_TYPE = 2 then 'Kart Yukleme'\r\n                    when REF_TRN_TYPE = 3 then 'Sinirli Kart Satis'\r\n                    when REF_TRN_TYPE = 4 then 'Istanbulkart Satis'\r\n                    when REF_TRN_TYPE = 5 then 'Dolum + Istanbulkart Satis'\r\n                    when REF_TRN_TYPE = 6 then 'Dijital Kart Yukleme'\r\n                    when REF_TRN_TYPE = 7 then 'Bireysel Internet Abonman Yukleme'\r\n                    when REF_TRN_TYPE = 8 then 'Mobil Abonman Yukleme'\r\n                    else cast(REF_TRN_TYPE as varchar(30))\r\nend                         as kuraciklama,\r\n                '002'                                                                  as kurumkod\r\nFROM [EPara].[TRN].[VPOS_PAYMENT] v\r\n         with (nolock)\r\n         INNER JOIN [EPara].[PRM].[CARD_BIN] c\r\n    with (nolock)\r\n                    on SUBSTRING(v.[PAN_MASKED], 1, 6) = c.BIN_NUMBER and COALESCE(c.RECORD_STATUS, 'A') = 'A'\r\n\r\n         left join EPara.CRD.CARD_MASTER as cm\r\n    with (NOLOCK) on cm.MIFARE_ID = v.MIFARE_ID\r\n\r\n\r\n         left join CustomerDb.dbo.Customers customer\r\n    WITH (NOLOCK) on cm.CUSTOMER_NUMBER = customer.Id\r\n         LEFT OUTER JOIN [CustomerDb].[dbo].[CustomerAddressNew] AS ca\r\n    WITH (NOLOCK) ON customer.[Id] = ca.Id and ca.RecordStatus = 'A' AND ca.AddressType = 7\r\n\r\n         LEFT JOIN [CustomerDb].[dbo].[CustomerIndividuals] as ci\r\n    WITH (NOLOCK) on ci.Id = customer.Id\r\n         LEFT JOIN [CustomerDb].[dbo].[PCountryCode_TBL] AS n\r\n    WITH (NOLOCK) ON n.CountryCode = ci.Nationality AND n.TENANT_CODE = 'BELBIM'\r\n\r\nWHERE COALESCE(v.RECORD_STATUS\r\n          , 'A') = 'A'\r\n  and v.PAYMENT_STATUS = 1\r\nand (cast(v.[INSERT_DATE]  as date) >= @TODAY and (cast(v.[INSERT_DATE]  as date) < @TOMORROW))\r\n   and v.VPOS_CHANNEL_CODE NOT IN ('BMATIK')\r\n  and REF_TRN_TYPE IN (2, 6)\r\n  AND V.INSERT_CHANNEL_CODE <> 'BMATIK'\r\n  and v.RECORD_ID is not null\r\nUNION ALL\r\n\r\n--3 - Kurumsal temsilci uzerinden gelen tl yuklemeleri\r\nSELECT distinct  'E'                                                                    as recordtype,\r\n                CII.RECORD_ID                                                          as lref,\r\n                'OH011'                                                                as islemturu,\r\n                'E'                                                                    as musterimi,\r\n                case\r\n                    when customer.TaxNo is null or len(customer.TaxNo) = 0 then '9999999999'\r\n                    else customer.TaxNo\r\n                    end                                                                as hestkvkn,\r\n                case\r\n                    when customer.FirmName is null or len(customer.FirmName) = 0 then 'XXXXXXXXXX'\r\n                    else customer.FirmName\r\n                    end                                                                as hestkunvan,\r\n\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\n                    end                                                                as hesgkad,\r\n\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n\r\n                    else UPPER(customer.Surname)\r\n                    end                                                                as hesgksoyad,\r\n\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\n                    END                                                                as hesgkkimliktipi,\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or\r\n                         len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo)) = 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo) end as hesgkkimlikno,\r\n                case\r\n                    when (n.ISOCode) is null or len(n.ISOCode) = 0 then 'XX'\r\n                    else\r\n                        n.ISOCode\r\n                    end                                                                as hesgkuyruk,\r\n                'XXX'                                                                  as hesgkadres,\r\n                'XXX'                                                                  as hesgkilceadi,\r\n                'XXX'                                                                  as hesgkpostakod,\r\n                case\r\n                    when UPPER(ca.CityCode) is not null then CONCAT('0', UPPER(ca.CityCode))\r\n                    else '000'\r\n                    end                                                                as hesgkilkod,\r\n                'XXX'                                                                  as hesgkiladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone]                  AS telefon WITH(NOLOCK ) WHERE telefon.Id = customer.Id\r\n                            and telefon.TelephoneType = 1\r\n                            and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nEND\r\nas hestel,\r\n                'XXX'                                                                                      as heseposta,\r\n                case when CTM.MIFARE_ID is null or len(CTM.MIFARE_ID)=0 then '00000' else CTM.MIFARE_ID\r\nend                        as hesno,\r\n                'TRY'                                                                                      as doviztip,\r\n                '9'                                                                                        as hsptip,\r\n\r\n\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\nend\r\nas kisiad,\r\n\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n\r\n                    else UPPER(customer.Surname)\r\nend\r\nas kisisoyad,\r\n\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\nEND\r\nas kisikimliktipi,\r\n\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or  len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo))= 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo)\r\nend                     as kisikimlikno,\r\n\r\n                case when (CII.INSERT_DATE) is null then '00000000' else cast(CII.INSERT_DATE as date)\r\nend as istar,\r\n\r\n                'XXX'                                                                                      as isknl,\r\n                'XXX'                                                                                      as bankaad,\r\n                case\r\n                    when (CII.TRN_AMOUNT) is null or len(CII.TRN_AMOUNT) = 0 then 0\r\n                    else\r\n                        CII.TRN_AMOUNT\r\nend\r\nas islemtutar,\r\n                case\r\n                    when (CII.TRN_AMOUNT) is null or len(CII.TRN_AMOUNT) = 0 then 0\r\n                    else\r\n                        CII.TRN_AMOUNT\r\nend\r\nas asilparatutar,\r\n                '949'                                                                                      as parabirim,\r\n                '0'                                                                                        as brutkomtut,\r\n                'XXXXX'                                                                                    as musaciklama,\r\n                CODE.TRN_CODE_DESCRIPTION                                                                  as kuraciklama,\r\n                '002'                                                                                      as kurumkod\r\nFROM EPara.TRN.CARD_INSTRUCTION CII WITH (nolock)\r\n         LEFT JOIN EPara.TRN.CARD_TRN_MASTER CTM\r\n    WITH (NOLOCK)\r\n                   ON CTM.RECORD_ID = CII.TRN_MASTER_ID\r\n         LEFT JOIN EPARA.PRM.TRN_CODE_MATRIX CODE\r\n    WITH (NOLOCK)\r\n                   ON CODE.TRN_CODE = CTM.TRN_CODE\r\n\r\n         left join EPara.CRD.CARD_MASTER as cm\r\n    with (NOLOCK) on cm.MIFARE_ID = CII.MIFARE_ID\r\n         left join CustomerDb.dbo.Customers customer\r\n    WITH (NOLOCK) on cm.CUSTOMER_NUMBER = customer.Id\r\n         LEFT OUTER JOIN [CustomerDb].[dbo].[CustomerAddressNew] AS ca\r\n    WITH (NOLOCK) ON customer.[Id] = ca.Id and ca.RecordStatus = 'A' AND ca.AddressType = 7\r\n\r\n         LEFT JOIN [CustomerDb].[dbo].[CustomerIndividuals] as ci\r\n    WITH (NOLOCK) on ci.Id = customer.Id\r\n         LEFT JOIN [CustomerDb].[dbo].[PCountryCode_TBL] AS n\r\n    WITH (NOLOCK) ON n.CountryCode = ci.Nationality AND n.TENANT_CODE = 'BELBIM'\r\n\r\nWHERE COALESCE(CII.RECORD_STATUS\r\n          , 'A') = 'A'\r\n  and CII.INSTRUCTION_TYPE IN (12 /*KURUMSAL TL TALIMATI*/, 17 /*Microkredi yukleme*/)\r\n  and (cast(CII.INSERT_DATE  as date) >= @TODAY and (cast(CII.INSERT_DATE  as date) < @TOMORROW))\r\n  and CII.INSTRUCTION_STATUS = 2\r\n  and CII.RECORD_ID is not null\r\nUNION ALL\r\n--Ibb askida kardel yuklemeleri\r\nSELECT distinct  'E'                                                                    as recordtype,\r\n                icd.RECORD_ID                                                          as lref,\r\n                'OH011'                                                                as islemturu,\r\n                'E'                                                                    as musterimi,\r\n                case\r\n                    when customer.TaxNo is null or len(customer.TaxNo) = 0 then '9999999999'\r\n                    else customer.TaxNo\r\n                    end                                                                as hestkvkn,\r\n                case\r\n                    when customer.FirmName is null or len(customer.FirmName) = 0 then 'XXXXXXXXXX'\r\n                    else customer.FirmName\r\n                    end                                                                as hestkunvan,\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\n                    end                                                                as hesgkad,\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Surname)\r\n                    end                                                                as hesgksoyad,\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\n                    END                                                                as hesgkkimliktipi,\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or\r\n                         len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo)) = 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo) end as hesgkkimlikno,\r\n                case\r\n                    when (n.ISOCode) is null or len(n.ISOCode) = 0 then 'XX'\r\n                    else\r\n                        n.ISOCode\r\n                    end                                                                as hesgkuyruk,\r\n                'XXX'                                                                  as hesgkadres,\r\n                'XXX'                                                                  as hesgkilceadi,\r\n                'XXX'                                                                  as hesgkpostakod,\r\n                case\r\n                    when UPPER(ca.CityCode) is not null then CONCAT('0', UPPER(ca.CityCode))\r\n                    else '000'\r\n                    end                                                                as hesgkilkod,\r\n                'XXX'                                                                  as hesgkiladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone]                  AS telefon WITH(NOLOCK ) WHERE telefon.Id = customer.Id\r\n                            and telefon.TelephoneType = 1\r\n                            and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nEND\r\nas hestel,\r\n                'XXX'                                                                                    as heseposta,\r\n                case when icd.MIFARE_ID is null or len(icd.MIFARE_ID)=0 then '00000' else icd.MIFARE_ID\r\nend                      as hesno,\r\n                'TRY'                                                                                    as doviztip,\r\n                '9'                                                                                      as hsptip,\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\nend\r\nas kisiad,\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Surname)\r\nend\r\nas kisisoyad,\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\nEND\r\nas kisikimliktipi,\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or  len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo))= 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo)\r\nend                   as kisikimlikno,\r\n                case when icd.UPDATE_DATE is null then '00000000' else cast(icd.UPDATE_DATE as date)\r\nend as istar,\r\n                'XXX'                                                                                    as isknl,\r\n                'XXX'                                                                                    as bankaad,\r\n                case\r\n                    when (icd.AMOUNT) is null or len(icd.AMOUNT) = 0 then 0\r\n                    else\r\n                        icd.AMOUNT\r\nend\r\nas islemtutar,\r\n                case\r\n                    when (icd.AMOUNT) is null or len(icd.AMOUNT) = 0 then 0\r\n                    else\r\n                        icd.AMOUNT\r\nend\r\nas asilparatutar,\r\n                '949'                                                                                    as parabirim,\r\n                '0'                                                                                      as brutkomtut,\r\n                'XXXXX'                                                                                  as musaciklama,\r\n                'Ibb Askida Paket Yukleme'                                                               as kuraciklama,\r\n                '002'                                                                                    as kurumkod\r\nFROM [EPara].[TRN].[IBB_CARD_DONATION] as icd\r\n         with (nolock)\r\n         LEFT JOIN [EPara].[TRN].[IBB_CARD_DONATION_TYPE] as icdt\r\n    with (nolock)\r\n                   ON icd.DONATION_CODE = icdt.DONATION_CODE\r\n         left join EPara.CRD.CARD_MASTER as cm\r\n    with (NOLOCK) on cm.MIFARE_ID = icd.MIFARE_ID\r\n         left join CustomerDb.dbo.Customers customer\r\n    WITH (NOLOCK) on cm.CUSTOMER_NUMBER = customer.Id\r\n         LEFT OUTER JOIN [CustomerDb].[dbo].[CustomerAddressNew] AS ca\r\n    WITH (NOLOCK) ON customer.[Id] = ca.Id and ca.RecordStatus = 'A' AND ca.AddressType = 7\r\n         LEFT JOIN [CustomerDb].[dbo].[CustomerIndividuals] as ci\r\n    WITH (NOLOCK) on ci.Id = customer.Id\r\n         LEFT JOIN [CustomerDb].[dbo].[PCountryCode_TBL] AS n\r\n    WITH (NOLOCK) ON n.CountryCode = ci.Nationality AND n.TENANT_CODE = 'BELBIM'\r\n\r\nWHERE icd.RECORD_STATUS = 'A'\r\n\r\nand (cast(icd.UPDATE_DATE as date) >= @TODAY and (cast(icd.UPDATE_DATE as date) < @TOMORROW))\r\n  and icd.DONATION_STATUS = 1\r\n  and icd.RECORD_ID is not null\r\n\r\nUNION ALL\r\n\r\nSELECT distinct  'E'                                                                    as recordtype,\r\n                CII.RECORD_ID                                                          as lref,\r\n                'OH011'                                                                as islemturu,\r\n                'E'                                                                    as musterimi,\r\n                case\r\n                    when customer.TaxNo is null or len(customer.TaxNo) = 0 then '9999999999'\r\n                    else customer.TaxNo\r\n                    end                                                                as hestkvkn,\r\n                case\r\n                    when customer.FirmName is null or len(customer.FirmName) = 0 then 'XXXXXXXXXX'\r\n                    else customer.FirmName\r\n                    end                                                                as hestkunvan,\r\n\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\n                    end                                                                as hesgkad,\r\n\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n\r\n                    else UPPER(customer.Surname)\r\n                    end                                                                as hesgksoyad,\r\n\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\n                    END                                                                as hesgkkimliktipi,\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or\r\n                         len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo)) = 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo) end as hesgkkimlikno,\r\n                case\r\n                    when (n.ISOCode) is null or len(n.ISOCode) = 0 then 'XX'\r\n                    else\r\n                        n.ISOCode\r\n                    end                                                                as hesgkuyruk,\r\n                'XXX'                                                                  as hesgkadres,\r\n                'XXX'                                                                  as hesgkilceadi,\r\n                'XXX'                                                                  as hesgkpostakod,\r\n                case\r\n                    when UPPER(ca.CityCode) is not null then CONCAT('0', UPPER(ca.CityCode))\r\n                    else '000'\r\n                    end                                                                as hesgkilkod,\r\n                'XXX'                                                                  as hesgkiladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone]                  AS telefon WITH (NOLOCK) WHERE telefon.Id = customer.Id\r\n                            and telefon.TelephoneType = 1\r\n                            and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nEND\r\nas hestel,\r\n                'XXX'                                                                                      as heseposta,\r\n                case when CTM.MIFARE_ID is null or len(CTM.MIFARE_ID)=0 then '00000' else CTM.MIFARE_ID\r\nend                        as hesno,\r\n                'TRY'                                                                                      as doviztip,\r\n                '9'                                                                                        as hsptip,\r\n\r\n\r\n                case\r\n                    when (customer.Name) is null or len(customer.Name) = 0 and customer.Name is null then 'XXXXXXXXXX'\r\n                    else UPPER(customer.Name)\r\nend\r\nas kisiad,\r\n\r\n                case\r\n                    when (customer.Surname) is null or len(customer.Surname) = 0 and customer.FirmName is null\r\n                        then 'XXXXXXXXXX'\r\n\r\n                    else UPPER(customer.Surname)\r\nend\r\nas kisisoyad,\r\n\r\n                case\r\n                    when customer.IdentityType = 1 THEN 4\r\n                    when customer.IdentityType = 2 THEN 5\r\n                    when customer.IdentityType = 3 THEN 3\r\n                    when customer.IdentityType = 4 THEN 1\r\n                    else 5\r\nEND\r\nas kisikimliktipi,\r\n\r\n                case\r\n                    when COALESCE(customer.CitizenshipNumber, customer.IdentityNo) is null or  len(COALESCE(customer.CitizenshipNumber, customer.IdentityNo))= 0 then '00000000'\r\n                    else COALESCE(customer.CitizenshipNumber, customer.IdentityNo)\r\nend                     as kisikimlikno,\r\n\r\n                case when (CII.INSERT_DATE) is null then '00000000' else cast(CII.INSERT_DATE as date)\r\nend as istar,\r\n\r\n                'XXX'                                                                                      as isknl,\r\n                'XXX'                                                                                      as bankaad,\r\n                case\r\n                    when (CII.TRN_AMOUNT) is null or len(CII.TRN_AMOUNT) = 0 then 0\r\n                    else\r\n                        CII.TRN_AMOUNT\r\nend\r\nas islemtutar,\r\n                case\r\n                    when (CII.TRN_AMOUNT) is null or len(CII.TRN_AMOUNT) = 0 then 0\r\n                    else\r\n                        CII.TRN_AMOUNT\r\nend\r\nas asilparatutar,\r\n                '949'                                                                                      as parabirim,\r\n                '0'                                                                                        as brutkomtut,\r\n                'XXXXX'                                                                                    as musaciklama,\r\n                tcm.TRN_CODE_DESCRIPTION                                                                  as kuraciklama,\r\n                '002'\r\nFROM EPara.TRN.BANK_INSTRUCTION_LOG as bil WITH (nolock)\r\n         LEFT JOIN EPara.TRN.CARD_INSTRUCTION CII WITH (nolock) ON bil.CARD_INSTRUCTION_ID = CII.RECORD_ID\r\n         LEFT JOIN EPara.TRN.CARD_TRN_MASTER CTM WITH (NOLOCK)\r\n                   ON CTM.RECORD_ID = CII.TRN_MASTER_ID and COALESCE(ctm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN [EPara].[PRM].[TRN_CODE_MATRIX] as tcm with (NOLOCK)\r\n                   on CTM.TRN_CODE = tcm.TRN_CODE and COALESCE(tcm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN [EPara].[POS].[TERMINAL_MASTER] tm WITH (nolock) ON CII.TERMINAL_NUMBER = tm.TERMINAL_NUMBER\r\n         LEFT JOIN EPara.MRC.MERCHANT_MASTER MM WITH (NOLOCK) ON MM.MERCHANT_NUMBER = tm.MERCHANT_NUMBER\r\n         INNER JOIN EPara.CRD.CARD_MASTER as cm with (NOLOCK) ON cm.MIFARE_ID = CII.MIFARE_ID\r\n         LEFT JOIN CustomerDb.dbo.Customers customer WITH (NOLOCK) on customer.Id = cm.CUSTOMER_NUMBER\r\n         LEFT JOIN CustomerDb.DBO.CUSTOMERS CUST WITH (NOLOCK) ON CUST.ID = MM.CUSTOMER_NUMBER\r\n\t     LEFT JOIN [CustomerDb].[dbo].[CustomerIndividuals] as ci    WITH (NOLOCK) on ci.Id = customer.Id\r\n\t    LEFT OUTER JOIN [CustomerDb].[dbo].[CustomerAddressNew] AS ca    WITH (NOLOCK) ON customer.[Id] = ca.Id and ca.RecordStatus = 'A' AND ca.AddressType = 7\r\n\t\tLEFT JOIN [CustomerDb].[dbo].[PCountryCode_TBL] AS n  WITH (NOLOCK) ON n.CountryCode = ci.Nationality AND n.TENANT_CODE = 'BELBIM'\r\nwhere COALESCE(bil.RECORD_STATUS, 'A') = 'A'\r\n  and (cast(CII.INSERT_DATE as date) >= @TODAY and (cast(CII.INSERT_DATE as date) < @TOMORROW))\r\n   and CII.INSTRUCTION_STATUS = 2 --basarili    --NOT IN (3) --iptal haric Talimatlar\r\n  and bil.CARD_INSTRUCTION_ID IS NOT NULL\r\n  and CII.INSTRUCTION_TYPE <> 17 -- MikroKredi dahil degil\r\n  and MM.CUSTOMER_NUMBER NOT IN (\r\n\t\t\t\t\t\t\t\t\t '1511' --BELBIM ELEKTRONIK PARA VE �deme HIZMETLERI,\r\n\t\t\t\t\t\t\t\t\t, '3715' --'ISTANBUL SPOR ETKINLIKLERI VE ISLETMECILIGI T.C.',\r\n\t\t\t\t\t\t\t\t\t, '2954' --TURKCELL �deme VE ELEKTRONIK PARA HIZ. A.S\r\n\t\t\t\t\t\t\t\t)";

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
                        List<string> musterimi = new List<string>();
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
                        List<string> kisiad = new List<string>();
                        List<string> kisisoyad = new List<string>();
                        List<string> kisikimliktipi = new List<string>();
                        List<string> kisikimlikno = new List<string>();
                        List<string> istar = new List<string>();
                        List<string> isknl = new List<string>();
                        List<string> bankaad = new List<string>();
                        List<string> islemtutar = new List<string>();
                        List<string> asilparatutar = new List<string>();
                        List<string> parabirim = new List<string>();
                        List<string> brutkomtut = new List<string>();
                        List<string> kuraciklama = new List<string>();
                        List<string> musaciklama = new List<string>();
                        List<string> KurumKod = new List<string>();



                        var columns = new Column[]
                        {
                            new Column<string>("recordtype"),
                            new Column<string>("lref"),
                            new Column<string>("islemturu"),
                            new Column<string>("musterimi"),
                            new Column<string>("hestkvkn"),
                            new Column<string>("hestkunvan"),
                            new Column<string>("hesgkad"),
                            new Column<string>("hesgksoyad"),
                            new Column<string>("hesgkkimliktipi"),
                            new Column<string>("hesgkkimlikno"),
                            new Column<string>("hesgkuyruk"),
                            new Column<string>("hesgkadres"),
                            new Column<string>("hesgkilceadi"),
                            new Column<string>("hesgkpostakod"),
                            new Column<string>("hesgkilkod"),
                            new Column<string>("hesgkiladi"),
                            new Column<string>("hestel"),
                            new Column<string>("heseposta"),
                            new Column<string>("hesno"),
                            new Column<string>("doviztip"),
                            new Column<string>("hsptip"),
                            new Column<string>("kisiad"),
                            new Column<string>("kisisoyad"),
                            new Column<string>("kisikimliktipi"),
                            new Column<string>("kisikimlikno"),
                            new Column<string>("istar"),
                            new Column<string>("isknl"),
                            new Column<string>("bankaad"),
                            new Column<string>("islemtutar"),
                            new Column<string>("asilparatutar"),
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
                            musterimi.Add(reader.GetString("musterimi"));
                            VergiKimlikNo.Add(reader.GetString("hestkvkn"));
                            TuzelKisilikUnvan.Add(reader.GetString("hestkunvan"));
                            Ad.Add(reader.GetString("hesgkad")); 
                            Soyad.Add(reader.GetString("hesgksoyad"));
                            KimlikTipi.Add(reader.GetInt32("hesgkkimliktipi").ToString());
                            KimlikNumarasi.Add(reader.GetString("hesgkkimlikno"));
                            Uyruk.Add(reader.GetString("hesgkuyruk"));
                            Adresi.Add(reader.GetString("hesgkadres"));
                            IlceAdi.Add(reader.GetString("hesgkilceadi"));
                            PostaKodu.Add(reader.GetString("hesgkpostakod"));
                            IlKodu.Add(reader.GetString("hesgkilkod"));
                            IlAdi.Add(reader.GetString("hesgkiladi"));
                            Telefon.Add(reader.GetString("hestel"));
                            Eposta.Add(reader.GetString("heseposta"));
                            HesapNo.Add(reader.GetString("hesno"));
                            DovizTipi.Add(reader.GetString("doviztip"));
                            HesapTipi.Add(reader.GetString("hsptip"));
                            kisiad.Add(reader.GetString("kisiad"));
                            kisisoyad.Add(reader.GetString("kisisoyad"));
                            kisikimliktipi.Add(reader.GetInt32("kisikimliktipi").ToString());
                            kisikimlikno.Add(reader.GetString("kisikimlikno"));
                            istar.Add(reader.GetDateTime("istar").ToString());
                            isknl.Add(reader.GetString("isknl"));
                            bankaad.Add(reader.GetString("bankaad"));
                            islemtutar.Add(reader.GetDecimal("islemtutar").ToString());
                            asilparatutar.Add(reader.GetDecimal("asilparatutar").ToString());
                            parabirim.Add(reader.GetString("parabirim"));
                            brutkomtut.Add(reader.GetString("brutkomtut"));
                            musaciklama.Add(reader.GetString("musaciklama"));
                            kuraciklama.Add(reader.GetString("kuraciklama"));
                            KurumKod.Add(reader.GetString("kurumkod"));

                        }
                        #endregion

                        ek5filename = FileNameHelper.GetDynamicFileName("KK002_EPHPYCNI_");

						using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ek5_output", ek5filename), columns);
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
                            objectIdWriter.WriteBatch(musterimi.ToArray());
                        }  //musterimi
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
                            objectIdWriter.WriteBatch(kisiad.ToArray());
                        } //kisiad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(kisisoyad.ToArray());
                        } //kisisoyad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(kisikimliktipi.ToArray());
                        } //kisikimliktipi
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(kisikimlikno.ToArray());
                        }//kisikimlikno
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(istar.ToArray());
                        }//istar
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(isknl.ToArray());
                        }//isknl 
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(bankaad.ToArray());
                        }//bankaad
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(islemtutar.ToArray());
                        }//islemtutar
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(asilparatutar.ToArray());
                        }//asilparatutar
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(parabirim.ToArray());
                        }//parabirim
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(brutkomtut.ToArray());
                        }//brutkomtut
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(musaciklama.ToArray());
                        }//musaciklama
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(kuraciklama.ToArray());
                        }//kuraciklama
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
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

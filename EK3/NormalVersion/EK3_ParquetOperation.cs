using Microsoft.Data.SqlClient;
using ParquetSharp;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Encoding = System.Text.Encoding;

namespace EK3.NormalVersion
{
    public class EK3_ParquetOperation
    {
		public static string ek3filename = "";
		public async Task GetParquetFile()
        {
            // Veritabanı bağlantı dizesi
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

            // (EK3) Sanal-Fiziksel Pos Formu
            // KK002_SFP_2020_12_20_0001.parquet
            string sqlQuery = "DECLARE @TODAY DATETIME = '20230602', @TOMORROW DATETIME = '20230603'\r\n\r\nSELECT distinct 'E'                                                   as recordtype,\r\n                ctm.RECORD_ID                                         as lref,\r\n                'FS0001'                                              as islemturu,\r\n                '1620044730'                                          as gonokvkn,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.Ş.'     as gonokunvan,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.Ş.'     as gonposbankaad,\r\n                '828'                                                 as gonposbankakod,\r\n                '0000000'                                             as gonuyeisyerino,\r\n                case\r\n                    when (tm.TERMINAL_NUMBER) is null or len(tm.TERMINAL_NUMBER) = 0\r\n                        then '0000000000'\r\n                    else tm.TERMINAL_NUMBER\r\n                    end                                               as gonteridno,\r\n                concat(case\r\n                           when (customer.Name) is null or len(customer.Name) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n                           else UPPER(customer.Name)\r\n                           end,\r\n                       case\r\n                           when (customer.Surname) is null or\r\n                                len(customer.Surname) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n\r\n                           else UPPER(customer.Surname)\r\n                           end)                                       as gonbankaad,\r\n                case\r\n                    when (customer.Id) is null or len(customer.Id) = 0\r\n                        then '0000000000'\r\n                    else customer.Id\r\n                    end\r\n                                                                      as gonbankakod,\r\n                case\r\n                    when (ctm.MIFARE_ID) is null or len(ctm.MIFARE_ID) = 0\r\n                        then '0000000000'\r\n\r\n                    else ctm.MIFARE_ID\r\n                    end                                                  goniban,\r\n                '000000000'                                           as gonhesno,\r\n                'E'                                                   as musterimi,\r\n                case\r\n                    when (CUST.TaxNo) is null or len(CUST.TaxNo) = 0\r\n                        then '0000000000'\r\n\r\n                    else CUST.TaxNo\r\n                    end                                               as altkvkn,\r\n                case\r\n                    when (CUST.FirmName) is null or len(CUST.FirmName) = 0\r\n                        then 'XXXXXXXXXXX'\r\n                    else CUST.FirmName\r\n                    end                                               as altkunvan,\r\n                '000000'                                              as algkkimlikno,\r\n                'XXXXX'                                               as algkad,\r\n                'XXXXX'                                               as algksoyad,\r\n                'XXXXX'                                               as alnosuzad,\r\n                '0'                                                   as alnosuzkimliktipi,\r\n                '00000'                                               as alnosuzkimlikno,\r\n                '00'                                                  as alnosuzulke,\r\n                'XXXXX'                                               as alnosuziladi,\r\n                'TR'                                                  as aluyruk,\r\n                'XXX'                                                 AS aladres,\r\n                'XXX'                                                 AS alilceadi,\r\n                '000'                                                 AS alpostakod,\r\n                '000'                                                 AS alilkod,\r\n                'XXX'                                                 AS aliladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon with (nolock)  WHERE telefon.Id = customer.Id\r\n                                    and telefon.TelephoneType = 1\r\n                                    and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon with (nolock)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nend             as altel,\r\n                        '0000'                                                      as alokhesno,\r\n                        '0000'                                                      as alokepara,\r\n                        '0000'                                                      as alokkartno,\r\n                        'XXXX'                                                      as albankaad,\r\n                        '0000'                                                      as albankakod,\r\n                        'XXXX'                                                      as alsubead,\r\n                        '0000'                                                      as aliban,\r\n                        '0000'                                                      as alhesno,\r\n                        '0000'                                                      as alkredikartno,\r\n                        case\r\n                            when cast(ctm.TRN_DATE as date) is null or len(cast(ctm.TRN_DATE as date)) = 0\r\n                                then '00000000'\r\n                            else cast(ctm.TRN_DATE as date)\r\nend\r\nas istar,\r\n                        '00000000'                                                  as odemetar,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas islemtutar,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas asiltutar,\r\n                        '949'                                                       as parabirim,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas bruttutar,\r\n                        'H'                                                         as sirketmi,\r\n                        '000'                                                       as sirketvkn,\r\n                        'XXX'                                                       AS sirketunvan,\r\n                        'XXX'                                                       as bruttahtutar,\r\n                        case\r\n                            when tcm.TRN_CODE_DESCRIPTION is null then 'XXX'\r\n                            else  tcm.TRN_CODE_DESCRIPTION\r\nend as kuraciklama,\r\n                        'XXX'                                                       as musaciklama,\r\n                        'KK002'                                                     as kurumkod\r\nFROM EPara.CRD.CARD_MASTER as cm with (NOLOCK)\r\n         INNER JOIN [EPara].[TRN].[CARD_TRN_MASTER] ctm WITH (nolock)\r\n                    ON ctm.MIFARE_ID = cm.MIFARE_ID and COALESCE(ctm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPARA.TRN.CARD_TRN_LOAD ctl WITH (NOLOCK)\r\n                   ON ctm.RECORD_ID = ctl.TRN_MASTER_ID and COALESCE(ctl.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPara.PRM.TRN_CODE_MATRIX AS tcm WITH (nolock)\r\n                   ON tcm.TRN_CODE = ctm.TRN_CODE and COALESCE(tcm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPARA.MRC.MERCHANT_MASTER mm WITH (NOLOCK) ON ctm.MERCHANT_NUMBER = mm.MERCHANT_NUMBER\r\n         LEFT JOIN EPara.POS.TERMINAL_MASTER as tm with (nolock) on ctm.TERMINAL_NUMBER = tm.TERMINAL_NUMBER\r\n         LEFT JOIN [EPara].[PRM].[TERMINAL_TYPE] as tt WITH (NOLOCK) ON tm.TERMINAL_TYPE = tt.TERMINAL_TYPE_CODE\r\n         LEFT JOIN CustomerDb.dbo.Customers customer with (nolock) on customer.Id = cm.CUSTOMER_NUMBER\r\n         LEFT JOIN CustomerDb.DBO.CUSTOMERS CUST WITH (NOLOCK) ON CUST.ID = MM.CUSTOMER_NUMBER\r\nWHERE COALESCE(cm.RECORD_STATUS, 'A') = 'A'\r\n  AND (\r\n        (\r\n                    ctm.TRN_STATUS in (1 /*'BASARILI'*/\r\n                    , 5 /*'ASKIDA BASARILI'*/\r\n                    )\r\n                AND ctm.TRN_CODE IN (\r\n                /*Harcama*/\r\n                                     '01080011000' --Harcama Kontorlu - Ulasim\r\n                , '51080011000' --Harcama Iade Kontorlu - Ulasim\r\n                , '01080061000' --Harcama Kontorlu QR - Ulasim\r\n                , '51080061000' /*Harcama Iade Kontorlu QR - Ulasim*/\r\n                , '05020010003' --Vizeleme Gorevi - Ucretli - Biletmatik\r\n                , '05020010305' --Vizeleme Gorevi - Kurumsal - Biletmatik\r\n                , '05020010006' --Vizeleme Gorevi - Kart Bedelli - Biletmatik\r\n                , '05050010003' --Vizeleme Gorevi - Ucretli - Akdom\r\n                , '05050010305' --Vizeleme Gorevi - Kurumsal - Akdom\r\n                , '05050010006' --Vizeleme Gorevi - Kart Bedelli - Akdom\r\n                )\r\n            )\r\n        OR (ctm.TRN_STATUS IN (1) and ctl.TRN_RESPONSE = 1 and\r\n            ctm.TRN_CODE IN ('01010010000')) --/*Harcama - POS - Sanal Kese*/\r\n        OR (ctm.TRN_CODE = '51010010000' and ctm.TRN_STATUS IN (1, 5)) --Harcama Iade - POS - Sanal Kese\r\n    )   and (cast(ctm.INSERT_DATE as date) >= @TODAY and (cast(ctm.INSERT_DATE as date) < @TOMORROW))\r\n  and  (cast(ctm.TRN_DATE as date) >= @TODAY and (cast(ctm.TRN_DATE as date) < @TOMORROW))\r\n\r\nUNION ALL\r\n\r\nSELECT distinct 'E'                                                   as recordtype,\r\n                ctm.RECORD_ID                                         as lref,\r\n                'FS0001'                                              as islemturu,\r\n                '1620044730'                                          as gonokvkn,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonokunvan,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonposbankaad,\r\n                '828'                                                 as gonposbankakod,\r\n                '0000000'                                             as gonuyeisyerino,\r\n                case\r\n                    when (tm.TERMINAL_NUMBER) is null or len(tm.TERMINAL_NUMBER) = 0\r\n                        then '0000000000'\r\n                    else tm.TERMINAL_NUMBER\r\n                    end                                               as gonteridno,\r\n                concat(case\r\n                           when (customer.Name) is null or len(customer.Name) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n                           else UPPER(customer.Name)\r\n                           end,\r\n                       case\r\n                           when (customer.Surname) is null or\r\n                                len(customer.Surname) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n                           else UPPER(customer.Surname)\r\n                           end)                                       as gonbankaad,\r\n                case\r\n                    when (customer.Id) is null or len(customer.Id) = 0\r\n                        then '0000000000'\r\n                    else customer.Id\r\n                    end                                               as gonbankakod,\r\n                case\r\n                    when (ctm.MIFARE_ID) is null or len(ctm.MIFARE_ID) = 0\r\n                        then '0000000000'\r\n                    else ctm.MIFARE_ID\r\n                    end                                                  goniban,\r\n                '000000000'                                           as gonhesno,\r\n                'E'                                                   as musterimi,\r\n                case\r\n                    when (CUST.TaxNo) is null or len(CUST.TaxNo) = 0\r\n                        then '0000000000'\r\n\r\n                    else CUST.TaxNo\r\n                    end                                               as altkvkn,\r\n                case\r\n                    when (CUST.FirmName) is null or len(CUST.FirmName) = 0\r\n                        then 'XXXXXXXXXX'\r\n                    else CUST.FirmName\r\n                    end                                               as altkunvan,\r\n                '000000'                                              as algkkimlikno,\r\n                'XXXXX'                                               as algkad,\r\n                'XXXXX'                                               as algksoyad,\r\n                'XXXXX'                                               as alnosuzad,\r\n                '0'                                                   as alnosuzkimliktipi,\r\n                '00000'                                               as alnosuzkimlikno,\r\n                '00'                                                  as alnosuzulke,\r\n                'XXXXX'                                               as alnosuziladi,\r\n                'TR'                                                  as aluyruk,\r\n                'XXX'                                                 AS aladres,\r\n                'XXX'                                                 AS alilceadi,\r\n                '000'                                                 AS alpostakod,\r\n                '000'                                                 AS alilkod,\r\n                'XXX'                                                 AS aliladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK) WHERE telefon.Id = customer.Id\r\n                                    and telefon.TelephoneType = 1\r\n                                    and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nend           as altel,\r\n                        '0000'                                                    as alokhesno,\r\n                        '0000'                                                    as alokepara,\r\n                        '0000'                                                    as alokkartno,\r\n                        'XXXX'                                                    as albankaad,\r\n                        '0000'                                                    as albankakod,\r\n                        'XXXX'                                                    as alsubead,\r\n                        '0000'                                                    as aliban,\r\n                        '0000'                                                    as alhesno,\r\n                        '0000'                                                    as alkredikartno,\r\n                      case\r\n                            when (  cast(ctm.TRN_DATE as date) ) is null or len(  cast(ctm.TRN_DATE as date) ) = 0\r\n                                then '00000000'\r\n                            else   cast(ctm.TRN_DATE as date)\r\nend\r\nas istar,\r\n                        '00000000'                                                as odemetar,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas islemtutar,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas islemtutar,\r\n                        '949'                                                     as parabirim,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas bruttutar,\r\n                        'H'                                                       as sirketmi,\r\n                        '000'                                                     as sirketvkn,\r\n                        'XXX'                                                     AS sirketunvan,\r\n                        'XXX'                                                     as bruttahtutar,\r\n                        case\r\n                            when tcm.TRN_CODE_DESCRIPTION is null then 'XXX'\r\n                            else concat('DOLUM - ', tcm.TRN_CODE_DESCRIPTION)\r\nend as kuraciklama,\r\n                        'XXX'                                                     as musaciklama,\r\n                        'KK002'                                                   as kurumkod\r\nFROM EPara.CRD.CARD_MASTER as cm with (NOLOCK)\r\n         INNER JOIN [EPara].[TRN].[CARD_TRN_MASTER] ctm WITH (nolock)\r\n                    ON ctm.MIFARE_ID = cm.MIFARE_ID and COALESCE(ctm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPARA.TRN.CARD_TRN_LOAD ctl WITH (NOLOCK)\r\n                   ON ctm.RECORD_ID = ctl.TRN_MASTER_ID and COALESCE(ctl.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPara.PRM.TRN_CODE_MATRIX AS tcm WITH (nolock)\r\n                   ON tcm.TRN_CODE = ctm.TRN_CODE and COALESCE(tcm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN EPARA.MRC.MERCHANT_MASTER mm WITH (NOLOCK) ON ctm.MERCHANT_NUMBER = mm.MERCHANT_NUMBER\r\n         LEFT JOIN EPara.POS.TERMINAL_MASTER as tm with (nolock) on ctm.TERMINAL_NUMBER = tm.TERMINAL_NUMBER\r\n         LEFT JOIN [EPara].[PRM].[TERMINAL_TYPE] as tt WITH (NOLOCK) ON tm.TERMINAL_TYPE = tt.TERMINAL_TYPE_CODE\r\n         LEFT JOIN CustomerDb.dbo.Customers customer WITH (NOLOCK) on customer.Id = cm.CUSTOMER_NUMBER\r\n         LEFT JOIN CustomerDb.DBO.CUSTOMERS CUST WITH (NOLOCK) ON CUST.ID = MM.CUSTOMER_NUMBER\r\nWHERE COALESCE(cm.RECORD_STATUS, 'A') = 'A'\r\n  and  (cast(ctm.INSERT_DATE as date) >= @TODAY and (cast(ctm.INSERT_DATE as date) < @TOMORROW))\r\n  and  (cast(ctm.TRN_DATE as date) >= @TODAY and (cast(ctm.TRN_DATE as date) < @TOMORROW))\r\n  -- Akdom- Pos- Bilatmatik yukleme ve Iadeleri\r\n  and (\r\n        (ctm.TRN_STATUS in (1, 5)\r\n            and ctm.TRN_CODE IN\r\n                ('02010011000', '02020011100', '02020011200', '02050011000', '52010011000'/*TL IADE*/, '52050011000'/*TL IADE*/\r\n                    -- ,'02050012000'/*DOLUM ABONMAN*/,'52050012000' /*ABONMAN IADE */\r\n                    , '07060111000'/* IBM BAKIYE AKTARIM*/\r\n                    ))\r\n        OR (ctm.TRN_STATUS = 4 AND ctl.TRN_RESPONSE = 1 AND\r\n            ctm.TRN_CODE IN ('02010011000'/*TL POS*/, '52010011000'/*TL POS IADE*/))\r\n    )\r\n\r\nUNION ALL\r\n\r\nSELECT distinct 'E'                                                   as recordtype,\r\n                V.RECORD_ID                                           as lref,\r\n                'FS0001'                                              as islemturu,\r\n                '1620044730'                                          as gonokvkn,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonokunvan,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonposbankaad,\r\n                '828'                                                 as gonposbankakod,\r\n                '0000000'                                             as gonuyeisyerino,\r\n                case\r\n                    when (v.VPOS_BANK_CODE) is null or len(v.VPOS_BANK_CODE) = 0\r\n                        then '0'\r\n                    else v.VPOS_BANK_CODE\r\n                    end                                               as gonteridno,\r\n                concat(case\r\n                           when (customer.Name) is null or len(customer.Name) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n                           else UPPER(customer.Name)\r\n                           end,\r\n                       case\r\n                           when (customer.Surname) is null or\r\n                                len(customer.Surname) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n                           else UPPER(customer.Surname)\r\n                           end)                                       as gonbankaad,\r\n                case\r\n                    when (customer.Id) is null or len(customer.Id) = 0\r\n                        then '0000000000'\r\n                    else customer.Id\r\n                    end                                               as gonbankakod,\r\n                case\r\n                    when (v.MIFARE_ID) is null or len(v.MIFARE_ID) = 0\r\n                        then '0000000000'\r\n                    else v.MIFARE_ID\r\n                    end                                                  goniban,\r\n                '000000000'                                           as gonhesno,\r\n                'E'                                                   as musterimi,\r\n                case\r\n                    when (customer.TaxNo) is null or len(customer.TaxNo) = 0\r\n                        then '0000000000'\r\n                    else customer.TaxNo\r\n                    end                                               as altkvkn,\r\n                case\r\n                    when (customer.FirmName) is null or len(customer.FirmName) = 0\r\n                        then '0000000000'\r\n                    else customer.FirmName\r\n                    end                                               as altkunvan,\r\n                '000000'                                              as algkkimlikno,\r\n                'XXXXX'                                               as algkad,\r\n                'XXXXX'                                               as algksoyad,\r\n                'XXXXX'                                               as alnosuzad,\r\n                '0'                                                   as alnosuzkimliktipi,\r\n                '00000'                                               as alnosuzkimlikno,\r\n                '00'                                                  as alnosuzulke,\r\n                'XXXXX'                                               as alnosuziladi,\r\n                'TR'                                                  as aluyruk,\r\n                'XXX'                                                 AS aladres,\r\n                'XXX'                                                 AS alilceadi,\r\n                '000'                                                 AS alpostakod,\r\n                '000'                                                 AS alilkod,\r\n                'XXX'                                                 AS aliladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK) WHERE telefon.Id = customer.Id\r\n                                    and telefon.TelephoneType = 1\r\n                                    and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nend          as altel,\r\n                        '0000'                                                   as alokhesno,\r\n                        '0000'                                                   as alokepara,\r\n                        '0000'                                                   as alokkartno,\r\n                        'XXXX'                                                   as albankaad,\r\n                        '0000'                                                   as albankakod,\r\n                        'XXXX'                                                   as alsubead,\r\n                        '0000'                                                   as aliban,\r\n                        '0000'                                                   as alhesno,\r\n                        '0000'                                                   as alkredikartno,\r\n                        case\r\n                            when (cast(v.INSERT_DATE as date) ) is null or len(cast(v.INSERT_DATE as date) ) = 0\r\n                                then '00000000'\r\n                            else cast(v.INSERT_DATE as date)\r\nend\r\nas istar,\r\n                        '00000000'                                               as odemetar,\r\n                        case\r\n                            when (v.[AMOUNT]) is null or len(v.[AMOUNT]) = 0\r\n                                then '0'\r\n                            else v.[AMOUNT]\r\nend\r\nas islemtutar,\r\n                        case\r\n                            when (v.[AMOUNT]) is null or len(v.[AMOUNT]) = 0\r\n                                then '0'\r\n                            else v.[AMOUNT]\r\nend\r\nas asiltutar,\r\n                        '949'                                                    as parabirim,\r\n                        case\r\n                            when (v.[AMOUNT]) is null or len(v.[AMOUNT]) = 0\r\n                                then '0'\r\n                            else v.[AMOUNT]\r\nend\r\nas bruttutar,\r\n                        'H'                                                      as sirketmi,\r\n                        '000'                                                    as sirketvkn,\r\n                        'XXX'                                                    AS sirketunvan,\r\n                        'XXX'                                                    as bruttahtutar,\r\n                        case\r\n                            when v.INSERT_CHANNEL_CODE is null then 'XXX'\r\n                            else  v.INSERT_CHANNEL_CODE\r\nend as kuraciklama,\r\n                        'XXX'                                                    as musaciklama,\r\n                        'KK002'                                                  as kurumkod\r\nFROM [EPara].[TRN].[VPOS_PAYMENT] v with (nolock)\r\n         INNER JOIN [EPara].[PRM].[CARD_BIN] c with (nolock)\r\n                    on SUBSTRING(v.[PAN_MASKED], 1, 6) = c.BIN_NUMBER and COALESCE(c.RECORD_STATUS, 'A') = 'A'\r\n         INNER JOIN EPara.CRD.CARD_MASTER as cm with (NOLOCK) ON cm.MIFARE_ID = v.MIFARE_ID\r\n         LEFT JOIN CustomerDb.dbo.Customers customer WITH (NOLOCK) on customer.Id = cm.CUSTOMER_NUMBER\r\nWHERE COALESCE(v.RECORD_STATUS, 'A') = 'A'\r\n  and v.PAYMENT_STATUS = 1\r\n  and (cast(v.[INSERT_DATE] as date) >= @TODAY and (cast(v.[INSERT_DATE] as date) < @TOMORROW))\r\n  and v.VPOS_CHANNEL_CODE NOT IN ('BMATIK')\r\n  and REF_TRN_TYPE IN (2, 6, 14)\r\n  AND V.INSERT_CHANNEL_CODE <> 'BMATIK'\r\n\r\nUNION ALL\r\n\r\n\r\nSELECT distinct 'E'                                                   as recordtype,\r\n                ctm.RECORD_ID                                         as lref,\r\n                'FS0001'                                              as islemturu,\r\n                '1620044730'                                          as gonokvkn,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonokunvan,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonposbankaad,\r\n                '828'                                                 as gonposbankakod,\r\n                '0000000'                                             as gonuyeisyerino,\r\n                case\r\n                    when (tm.TERMINAL_NUMBER) is null or len(tm.TERMINAL_NUMBER) = 0\r\n                        then '0'\r\n                    else tm.TERMINAL_NUMBER\r\n                    end                                               as gonteridno,\r\n                concat(case\r\n                           when (customer.Name) is null or len(customer.Name) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n                           else UPPER(customer.Name)\r\n                           end,\r\n                       case\r\n                           when (customer.Surname) is null or\r\n                                len(customer.Surname) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n                           else UPPER(customer.Surname)\r\n                           end)                                       as gonbankaad,\r\n                case\r\n                    when (customer.Id) is null or len(customer.Id) = 0\r\n                        then '0000000000'\r\n                    else customer.Id\r\n                    end                                               as gonbankakod,\r\n                case\r\n                    when (ctm.MIFARE_ID) is null or len(ctm.MIFARE_ID) = 0\r\n                        then '0'\r\n                    else ctm.MIFARE_ID\r\n                    end                                                  goniban,\r\n                '000000000'                                           as gonhesno,\r\n                'E'                                                   as musterimi,\r\n                case\r\n                    when (CUST.TaxNo) is null or len(CUST.TaxNo) = 0\r\n                        then '0'\r\n\r\n                    else CUST.TaxNo\r\n                    end                                               as altkvkn,\r\n                case\r\n                    when (CUST.FirmName) is null or len(CUST.FirmName) = 0\r\n                        then '0'\r\n\r\n                    else CUST.FirmName\r\n                    end                                               as altkunvan,\r\n                '000000'                                              as algkkimlikno,\r\n                'XXXXX'                                               as algkad,\r\n                'XXXXX'                                               as algksoyad,\r\n                'XXXXX'                                               as alnosuzad,\r\n                '0'                                                   as alnosuzkimliktipi,\r\n                '00000'                                               as alnosuzkimlikno,\r\n                '00'                                                  as alnosuzulke,\r\n                'XXXXX'                                               as alnosuziladi,\r\n                'TR'                                                  as aluyruk,\r\n                'XXX'                                                 AS aladres,\r\n                'XXX'                                                 AS alilceadi,\r\n                '000'                                                 AS alpostakod,\r\n                '000'                                                 AS alilkod,\r\n                'XXX'                                                 AS aliladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK) WHERE telefon.Id = customer.Id\r\n                                    and telefon.TelephoneType = 1\r\n                                    and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nend   as altel,\r\n                        '0000'                                            as alokhesno,\r\n                        '0000'                                            as alokepara,\r\n                        '0000'                                            as alokkartno,\r\n                        'XXXX'                                            as albankaad,\r\n                        '0000'                                            as albankakod,\r\n                        'XXXX'                                            as alsubead,\r\n                        '0000'                                            as aliban,\r\n                        '0000'                                            as alhesno,\r\n                        '0000'                                            as alkredikartno,\r\n                        case\r\n                            when (cast(ctm.TRN_DATE as date) ) is null or len(cast(ctm.TRN_DATE as date) ) = 0\r\n                                then '00000000'\r\n                            else cast(ctm.TRN_DATE as date)\r\nend\r\nas istar,\r\n                        '00000000'                                        as odemetar,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas islemtutar,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas asiltutar,\r\n                        '949'                                             as parabirim,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas bruttutar,\r\n                        'H'                                               as sirketmi,\r\n                        '000'                                             as sirketvkn,\r\n                        'XXX'                                             AS sirketunvan,\r\n                        'XXX'                                             as bruttahtutar,\r\n                        'DOLUM - Kurumsal TL talimati yukleme'            as kuraciklama,\r\n                        'XXX'                                             as musaciklama,\r\n                        'KK002'                                           as kurumkod\r\nFROM EPara.TRN.CARD_INSTRUCTION CI WITH (nolock)\r\n         LEFT JOIN EPara.TRN.CARD_TRN_MASTER CTM WITH (NOLOCK) ON CTM.RECORD_ID = CI.TRN_MASTER_ID\r\n         LEFT JOIN EPARA.PRM.TRN_CODE_MATRIX CODE WITH (NOLOCK) ON CODE.TRN_CODE = CTM.TRN_CODE\r\n         LEFT JOIN [EPara].[POS].[TERMINAL_MASTER] tm WITH (nolock) ON CI.TERMINAL_NUMBER = tm.TERMINAL_NUMBER\r\n         LEFT JOIN EPara.MRC.MERCHANT_MASTER MM WITH (NOLOCK) ON MM.MERCHANT_NUMBER = tm.MERCHANT_NUMBER\r\n         INNER JOIN EPara.CRD.CARD_MASTER as cm with (NOLOCK) ON cm.MIFARE_ID = CI.MIFARE_ID\r\n         LEFT JOIN CustomerDb.dbo.Customers customer WITH (NOLOCK) on customer.Id = cm.CUSTOMER_NUMBER\r\n         LEFT JOIN CustomerDb.DBO.CUSTOMERS CUST WITH (NOLOCK) ON CUST.ID = MM.CUSTOMER_NUMBER\r\nwhere COALESCE(CI.RECORD_STATUS, 'A') = 'A'\r\n  and CI.INSTRUCTION_TYPE IN (12 /*KURUMSAL TL TALIMATI*/, 17 /*Microkredi yukleme*/)\r\nand  (cast(CI.INSERT_DATE as date) >= @TODAY and (cast(CI.INSERT_DATE as date) < @TOMORROW))\r\nand CI.INSTRUCTION_STATUS = 2 -- 'Basarili'\r\n\r\n\r\nUNION ALL\r\n\r\n\r\nSELECT distinct 'E'                                                   as recordtype,\r\n                ctm.RECORD_ID                                         as lref,\r\n                'FS0001'                                              as islemturu,\r\n                '1620044730'                                          as gonokvkn,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonokunvan,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonposbankaad,\r\n                '828'                                                 as gonposbankakod,\r\n                '0000000'                                             as gonuyeisyerino,\r\n                case\r\n                    when (tm.TERMINAL_NUMBER) is null or len(tm.TERMINAL_NUMBER) = 0\r\n                        then '0'\r\n\r\n                    else tm.TERMINAL_NUMBER\r\n                    end                                               as gonteridno,\r\n                concat(case\r\n                           when (customer.Name) is null or len(customer.Name) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n                           else UPPER(customer.Name)\r\n                           end,\r\n                       case\r\n                           when (customer.Surname) is null or\r\n                                len(customer.Surname) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n\r\n                           else UPPER(customer.Surname)\r\n                           end)                                       as gonbankaad,\r\n                case\r\n                    when (customer.Id) is null or len(customer.Id) = 0\r\n                        then '0000000000'\r\n                    else customer.Id\r\n                    end                                               as gonbankakod,\r\n                ctm.MIFARE_ID                                            goniban,\r\n                '000000000'                                           as gonhesno,\r\n                'E'                                                   as musterimi,\r\n                case\r\n                    when (CUST.TaxNo) is null or len(CUST.TaxNo) = 0\r\n                        then '0'\r\n\r\n                    else CUST.TaxNo\r\n                    end                                               as altkvkn,\r\n                case\r\n                    when (CUST.FirmName) is null or len(CUST.FirmName) = 0\r\n                        then '0'\r\n\r\n                    else CUST.FirmName\r\n                    end                                               as altkunvan,\r\n                '000000'                                              as algkkimlikno,\r\n                'XXXXX'                                               as algkad,\r\n                'XXXXX'                                               as algksoyad,\r\n                'XXXXX'                                               as alnosuzad,\r\n                '0'                                                   as alnosuzkimliktipi,\r\n                '00000'                                               as alnosuzkimlikno,\r\n                '00'                                                  as alnosuzulke,\r\n                'XXXXX'                                               as alnosuziladi,\r\n                'TR'                                                  as aluyruk,\r\n                'XXX'                                                 AS aladres,\r\n                'XXX'                                                 AS alilceadi,\r\n                '000'                                                 AS alpostakod,\r\n                '000'                                                 AS alilkod,\r\n                'XXX'                                                 AS aliladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK) WHERE telefon.Id = customer.Id\r\n                                    and telefon.TelephoneType = 1\r\n                                    and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nend             as altel,\r\n                        '0000'                                                      as alokhesno,\r\n                        '0000'                                                      as alokepara,\r\n                        '0000'                                                      as alokkartno,\r\n                        'XXXX'                                                      as albankaad,\r\n                        '0000'                                                      as albankakod,\r\n                        'XXXX'                                                      as alsubead,\r\n                        '0000'                                                      as aliban,\r\n                        '0000'                                                      as alhesno,\r\n                        '0000'                                                      as alkredikartno,\r\n                         case\r\n                            when (cast(ctm.TRN_DATE as date)) is null or len(cast(ctm.TRN_DATE as date)) = 0\r\n                                then '00000000'\r\n\r\n                            else cast(ctm.TRN_DATE as date)\r\nend\r\nas istar,\r\n                        '00000000'                                                  as odemetar,\r\n\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas islemtutar,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas asiltutar,\r\n                        '949'                                                       as parabirim,\r\n                        case\r\n                            when (ctm.TRN_AMOUNT) is null or len(ctm.TRN_AMOUNT) = 0\r\n                                then '0'\r\n\r\n                            else ctm.TRN_AMOUNT\r\nend\r\nas bruttutar,\r\n                        'H'                                                         as sirketmi,\r\n                        '000'                                                       as sirketvkn,\r\n                        'XXX'                                                       AS sirketunvan,\r\n                        'XXX'                                                       as bruttahtutar,\r\n                        case\r\n                            when tcm.TRN_CODE_DESCRIPTION is null then 'XXX'\r\n                            else tcm.TRN_CODE_DESCRIPTION\r\nend as kuraciklama,\r\n                        'XXX'                                                       as musaciklama,\r\n                        'KK002'                                                     as kurumkod\r\nFROM EPara.TRN.BANK_INSTRUCTION_LOG as bil WITH (nolock)\r\n         LEFT JOIN EPara.TRN.CARD_INSTRUCTION CI WITH (nolock) ON bil.CARD_INSTRUCTION_ID = CI.RECORD_ID\r\n         LEFT JOIN EPara.TRN.CARD_TRN_MASTER CTM WITH (NOLOCK)\r\n                   ON CTM.RECORD_ID = CI.TRN_MASTER_ID and COALESCE(ctm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN [EPara].[PRM].[TRN_CODE_MATRIX] as tcm with (NOLOCK)\r\n                   on CTM.TRN_CODE = tcm.TRN_CODE and COALESCE(tcm.RECORD_STATUS, 'A') = 'A'\r\n         LEFT JOIN [EPara].[POS].[TERMINAL_MASTER] tm WITH (nolock) ON CI.TERMINAL_NUMBER = tm.TERMINAL_NUMBER\r\n         LEFT JOIN EPara.MRC.MERCHANT_MASTER MM WITH (NOLOCK) ON MM.MERCHANT_NUMBER = tm.MERCHANT_NUMBER\r\n         INNER JOIN EPara.CRD.CARD_MASTER as cm with (NOLOCK) ON cm.MIFARE_ID = CI.MIFARE_ID\r\n         LEFT JOIN CustomerDb.dbo.Customers customer WITH (NOLOCK) on customer.Id = cm.CUSTOMER_NUMBER\r\n         LEFT JOIN CustomerDb.DBO.CUSTOMERS CUST WITH (NOLOCK) ON CUST.ID = MM.CUSTOMER_NUMBER\r\nwhere COALESCE(bil.RECORD_STATUS, 'A') = 'A'\r\n  and (cast(CI.INSERT_DATE as date) >= @TODAY and (cast(CI.INSERT_DATE as date) < @TOMORROW))\r\n  and CI.INSTRUCTION_STATUS = 2 --basarili    --NOT IN (3) --iptal haric Talimatlar\r\n  and bil.CARD_INSTRUCTION_ID IS NOT NULL\r\n  and CI.INSTRUCTION_TYPE <> 17 -- MikroKredi dahil degil\r\n  and MM.CUSTOMER_NUMBER NOT IN (\r\n                                 '1511' --BELBIM ELEKTRONIK PARA VE Ödeme HIZMETLERI,\r\n    , '3715' --'ISTANBUL SPOR ETKINLIKLERI VE ISLETMECILIGI T.C.',\r\n    , '2954' --TURKCELL Ödeme VE ELEKTRONIK PARA HIZ. A.S\r\n    )                           --haric\r\n\r\nUNION ALL\r\n\r\nSELECT distinct 'E'                                                   as recordtype,\r\n                icd.RECORD_ID                                         as lref,\r\n                'FS0001'                                              as islemturu,\r\n                '1620044730'                                          as gonokvkn,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonokunvan,\r\n                'Belbim Elektronik Para ve Ödeme Hizmetleri A.S.'     as gonposbankaad,\r\n                '828'                                                 as gonposbankakod,\r\n                '0000000'                                             as gonuyeisyerino,\r\n                '0000'                                                as gonteridno,\r\n                concat(case\r\n                           when (customer.Name) is null or len(customer.Name) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n                           else UPPER(customer.Name)\r\n                           end,\r\n                       case\r\n                           when (customer.Surname) is null or\r\n                                len(customer.Surname) = 0 and customer.FirmName is null\r\n                               then 'XXXXXXXXXX'\r\n\r\n                           else UPPER(customer.Surname)\r\n                           end)                                       as gonbankaad,\r\n                case\r\n                    when (customer.Id) is null or len(customer.Id) = 0\r\n                        then '0000000000'\r\n                    else customer.Id\r\n                    end                                               as gonbankakod,\r\n                case\r\n                    when (icd.MIFARE_ID) is null or len(icd.MIFARE_ID) = 0\r\n                        then '00000000'\r\n\r\n                    else icd.MIFARE_ID\r\n                    end                                                  goniban,\r\n                '000000000'                                           as gonhesno,\r\n                'E'                                                   as musterimi,\r\n                case\r\n                    when (customer.TaxNo) is null or len(customer.TaxNo) = 0\r\n                        then '00000000'\r\n\r\n                    else customer.TaxNo\r\n                    end                                               as altkvkn,\r\n                case\r\n                    when (customer.FirmName) is null or len(customer.FirmName) = 0\r\n                        then '00000000'\r\n\r\n                    else customer.FirmName\r\n                    end                                               as altkunvan,\r\n                '000000'                                              as algkkimlikno,\r\n                'XXXXX'                                               as algkad,\r\n                'XXXXX'                                               as algksoyad,\r\n                'XXXXX'                                               as alnosuzad,\r\n                '0'                                                   as alnosuzkimliktipi,\r\n                '00000'                                               as alnosuzkimlikno,\r\n                '00'                                                  as alnosuzulke,\r\n                'XXXXX'                                               as alnosuziladi,\r\n                'TR'                                                  as aluyruk,\r\n                'XXX'                                                 AS aladres,\r\n                'XXX'                                                 AS alilceadi,\r\n                '000'                                                 AS alpostakod,\r\n                '000'                                                 AS alilkod,\r\n                'XXX'                                                 AS aliladi,\r\n                case\r\n                    when (SELECT TOP(1) CompleteTelephoneNumber\r\n                          FROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK) WHERE telefon.Id = customer.Id\r\n                                    and telefon.TelephoneType = 1\r\n                                    and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC) is null then '999999999999'\r\n    ELSE (\r\nSELECT TOP (1) CompleteTelephoneNumber\r\nFROM [CustomerDb].[dbo].[CustomerTelephone] AS telefon WITH (NOLOCK)\r\nWHERE telefon.Id = customer.Id\r\n  and telefon.TelephoneType = 1\r\n  and telefon.RecordStatus = 'A'\r\nORDER BY telefon.RecordTime DESC)\r\nend   as altel,\r\n                        '0000'                                            as alokhesno,\r\n                        '0000'                                            as alokepara,\r\n                        '0000'                                            as alokkartno,\r\n                        'XXXX'                                            as albankaad,\r\n                        '0000'                                            as albankakod,\r\n                        'XXXX'                                            as alsubead,\r\n                        '0000'                                            as aliban,\r\n                        '0000'                                            as alhesno,\r\n                        '0000'                                            as alkredikartno,\r\n                         case\r\n                            when (cast(icd.UPDATE_DATE as date) ) is null or len(cast(icd.UPDATE_DATE as date) ) = 0\r\n                                then '00000000'\r\n\r\n                            else cast(icd.UPDATE_DATE as date)\r\nend\r\nas istar,\r\n                        '00000000'                                        as odemetar,\r\n                        case\r\n                            when (icd.AMOUNT) is null or len(icd.AMOUNT) = 0\r\n                                then '0'\r\n\r\n                            else icd.AMOUNT\r\nend\r\nas islemtutar,\r\n                        case\r\n                            when (icd.AMOUNT) is null or len(icd.AMOUNT) = 0\r\n                                then '0'\r\n\r\n                            else icd.AMOUNT\r\nend\r\nas asiltutar,\r\n                        '949'                                             as parabirim,\r\n                        case\r\n                            when (icd.AMOUNT) is null or len(icd.AMOUNT) = 0\r\n                                then '0'\r\n\r\n                            else icd.AMOUNT\r\nend\r\nas bruttutar,\r\n                        'H'                                               as sirketmi,\r\n                        '000'                                             as sirketvkn,\r\n                        'XXX'                                             AS sirketunvan,\r\n                        'XXX'                                             as bruttahtutar,\r\n                        'DOLUM - IBB Askida destek Paketleri'             as kuraciklama,\r\n                        'XXX'                                             as musaciklama,\r\n                        'KK002'                                           as kurumkod\r\nFROM [EPara].[TRN].[IBB_CARD_DONATION] as icd with (nolock)\r\n         LEFT JOIN [EPara].[TRN].[IBB_CARD_DONATION_TYPE] as icdt with (nolock)\r\n                   ON icd.DONATION_CODE = icdt.DONATION_CODE\r\n         INNER JOIN EPara.CRD.CARD_MASTER as cm with (NOLOCK) ON cm.MIFARE_ID = icd.MIFARE_ID\r\n         LEFT JOIN CustomerDb.dbo.Customers customer on customer.Id = cm.CUSTOMER_NUMBER\r\nwhere icd.RECORD_STATUS = 'A'\r\nand\r\n(cast(icd.UPDATE_DATE as date) >= @TODAY and (cast(icd.UPDATE_DATE as date) < @TOMORROW))\r\nand icd.DONATION_STATUS = 1";

      
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
                        List<string> gonposbankaad = new List<string>();
                        List<string> gonposbankakod = new List<string>();
                        List<string> gonuyelikno = new List<string>();
                        List<string> gonterno = new List<string>();
                        List<string> gonbankaad = new List<string>();
                        List<string> gonbankakod = new List<string>();
                        List<string> goniban = new List<string>();
                        List<string> gonhesno = new List<string>();
                        List<string> musterimi = new List<string>();
                        List<string> altkvkn = new List<string>();
                        List<string> altkunvan = new List<string>();
                        List<string> algkkimlikno = new List<string>();
                        List<string> algkad = new List<string>();
                        List<string> algksoyad = new List<string>();
                        List<string> alnosuzad = new List<string>();
                        List<string> alnosuzkimliktipi = new List<string>();
                        List<string> alnosuzkimlikno = new List<string>();
                        List<string> alnosuzulke = new List<string>();
                        List<string> alnosuziladi = new List<string>();
                        List<string> aluyruk = new List<string>();
                        List<string> aladres = new List<string>();
                        List<string> alilceadi = new List<string>();
                        List<string> alpostakod = new List<string>();
                        List<string> alilkod = new List<string>();
                        List<string> aliladi = new List<string>();
                        List<string> altel = new List<string>();
                        List<string> alokhesno = new List<string>();
                        List<string> alokepara = new List<string>();
                        List<string> alokkartno = new List<string>();
                        List<string> albankaad = new List<string>();
                        List<string> albankakod = new List<string>();
                        List<string> alsubead = new List<string>();
                        List<string> aliban = new List<string>();
                        List<string> alhesno = new List<string>();
                        List<string> alkredikartno = new List<string>();
                        List<string> istar = new List<string>();
                        List<string> odemetar = new List<string>();
                        List<string> islemtutar = new List<string>();
                        List<string> asiltutar = new List<string>();
                        List<string> parabirim = new List<string>();
                        List<string> bruttutar = new List<string>();
                        List<string> sirketmi = new List<string>();
                        List<string> sirketvkn = new List<string>();
                        List<string> sirketunvan = new List<string>();
                        List<string> bruttahtutar = new List<string>();
                        List<string> kuraciklama = new List<string>();
                        List<string> musaciklama = new List<string>();
                        List<string> kurumkod = new List<string>();

                        var columns = new Column[]
                        {
                            new Column<string>("recordtype"),
                            new Column<string>("lref"),
                            new Column<string>("islemturu"),
                            new Column<string>("gonokvkn"),
                            new Column<string>("gonokunvan"),
                            new Column<string>("gonposbankaad"),
                            new Column<string>("gonposbankakod"),
                            new Column<string>("gonuyeisyerino"),
                            new Column<string>("gonteridno"),
                            new Column<string>("gonbankaad"),
                            new Column<string>("gonbankakod"),
                            new Column<string>("goniban"),
                            new Column<string>("gonhesno"),
                            new Column<string>("musterimi"),
                            new Column<string>("altkvkn"),
                            new Column<string>("altkunvan"),
                            new Column<string>("algkkimlikno"),
                            new Column<string>("algkad"),
                            new Column<string?>("algksoyad"),
                            new Column<string>("alnosuzad"),
                            new Column<string>("alnosuzkimliktipi"),
                            new Column<string>("alnosuzkimlikno"),
                            new Column<string>("alnosuzulke"),
                            new Column<string>("alnosuziladi"),
                            new Column<string>("aluyruk"),
                            new Column<string>("aladres"),
                            new Column<string>("alilceadi"),
                            new Column<string>("alpostakod"),
                            new Column<string>("alilkod"),
                            new Column<string>("aliladi"),
                            new Column<string>("altel"),
                            new Column<string>("alokhesno"),
                            new Column<string>("alokepara"),
                            new Column<string>("alokkartno"),
                            new Column<string>("albankaad"),
                            new Column<string>("albankakod"),
                            new Column<string>("alsubead"),
                            new Column<string>("aliban"),
                            new Column<string>("alhesno"),
                            new Column<string>("alkredikartno"),
                            new Column<string>("istar"),
                            new Column<string>("odemetar"),
                            new Column<string>("islemtutar"),
                            new Column<string>("asiltutar"),
                            new Column<string>("parabirim"),
                            new Column<string>("bruttutar"),
                            new Column<string>("sirketmi"),
                            new Column<string>("sirketvkn"),
                            new Column<string>("sirketunvan"),
                            new Column<string>("bruttahtutar"),
                            new Column<string>("kuraciklama"),
                            new Column<string>("musaciklama"),
                            new Column<string>("kurumkod")

                        };
                        #region row_reader_area
                        while (await reader.ReadAsync())
                        {
                            RecordType.Add(reader.GetString("recordtype"));
                            LocalRef.Add(reader.GetInt64("lref").ToString());
                            İslemTuruKodu.Add(reader.GetString("islemturu"));
                            VergiKimlikNo.Add(reader.GetString("gonokvkn"));
							TuzelKisilikUnvan.Add(reader.GetString("gonokunvan"));
                            gonposbankaad.Add(reader.GetString("gonposbankaad"));
                            gonposbankakod.Add(reader.GetString("gonposbankakod"));
                            gonuyelikno.Add(reader.GetString("gonuyeisyerino"));
                            gonterno.Add(reader.GetString("gonteridno"));
                            gonbankaad.Add(reader.GetString("gonbankaad"));
                            gonbankakod.Add(reader.GetInt64("gonbankakod").ToString());
                            goniban.Add(reader.GetString("goniban"));
                            gonhesno.Add(reader.GetString("gonhesno"));
                            musterimi.Add(reader.GetString("musterimi"));
                            altkvkn.Add(reader.GetString("altkvkn"));
                            altkunvan.Add(reader.GetString("altkunvan"));
                            algkkimlikno.Add(reader.GetString("algkkimlikno"));
                            algkad.Add(reader.GetString("algkad"));
                            algksoyad.Add(reader.GetString("algksoyad"));
                            alnosuzad.Add(reader.GetString("alnosuzad"));
                            alnosuzkimliktipi.Add(reader.GetString("alnosuzkimliktipi"));
                            alnosuzkimlikno.Add(reader.GetString("alnosuzkimlikno"));
                            alnosuzulke.Add(reader.GetString("alnosuzulke"));
                            alnosuziladi.Add(reader.GetString("alnosuziladi"));
                            aluyruk.Add(reader.GetString("aluyruk"));
                            aladres.Add(reader.GetString("aladres"));
                            alilceadi.Add(reader.GetString("alilceadi"));
                            alpostakod.Add(reader.GetString("alpostakod"));
                            alilkod.Add(reader.GetString("alilkod"));
                            aliladi.Add(reader.GetString("aliladi"));
                            altel.Add(reader.GetString("altel"));
                            alokhesno.Add(reader.GetString("alokhesno"));
                            alokepara.Add(reader.GetString("alokepara"));
                            alokkartno.Add(reader.GetString("alokkartno"));
                            albankaad.Add(reader.GetString("albankaad"));
                            albankakod.Add(reader.GetString("albankakod"));
                            alsubead.Add(reader.GetString("alsubead"));
                            aliban.Add(reader.GetString("aliban"));
                            alhesno.Add(reader.GetString("alhesno"));
                            alkredikartno.Add(reader.GetString("alkredikartno"));
                            istar.Add(reader.GetDateTime("istar").ToString());
                            odemetar.Add(reader.GetString("odemetar"));
                            islemtutar.Add(reader.GetDecimal("islemtutar").ToString());
                            asiltutar.Add(reader.GetDecimal("asiltutar").ToString());
                            parabirim.Add(reader.GetString("parabirim"));
                            bruttutar.Add(reader.GetDecimal("bruttutar").ToString());
                            sirketmi.Add(reader.GetString("sirketmi"));
                            sirketvkn.Add(reader.GetString("sirketvkn"));
                            sirketunvan.Add(reader.GetString("sirketunvan"));
                            bruttahtutar.Add(reader.GetString("bruttahtutar"));
                            kuraciklama.Add(reader.GetString("kuraciklama"));
                            musaciklama.Add(reader.GetString("musaciklama"));
                            kurumkod.Add(reader.GetString("kurumkod"));


                        }
						#endregion

						ek3filename = GetDynamicFileName();

						using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ek3_output", ek3filename), columns);
                        using var rowGroup = file.AppendRowGroup();
                        #region parquet_writer

                        /*RecordType*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
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

                        /*gonposbankaad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(gonposbankaad.ToArray());
                        }

                        /*gonposbankakod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(gonposbankakod.ToArray());
                        }

                        /*gonuyelikno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(gonuyelikno.ToArray());
                        }

                        /*gonterno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(gonterno.ToArray());
                        }

                        /*gonbankaad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(gonbankaad.ToArray());
                        }

                        /*gonbankakod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(gonbankakod.ToArray());
                        }

                        /*goniban*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(goniban.ToArray());
                        }

                        /*gonhesno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(gonhesno.ToArray());
                        }

                        /*musterimi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(musterimi.ToArray());
                        }

                        /*altkvkn*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(altkvkn.ToArray());
                        }

                        /*altkunvan*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(altkunvan.ToArray());
                        }

                        /*algkkimlikno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(algkkimlikno.ToArray());
                        }

                        /*algkad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(algkad.ToArray());
                        }

                        /*algksoyad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(algksoyad.ToArray());
                        }

                        /*alnosuzad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alnosuzad.ToArray());
                        }

                        /*alnosuzkimliktipi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alnosuzkimliktipi.ToArray());
                        }

                        /*alnosuzkimlikno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alnosuzkimlikno.ToArray());
                        }

                        /*alnosuzulke*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alnosuzulke.ToArray());
                        }

                        /*alnosuziladi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alnosuziladi.ToArray());
                        }

                        /*aluyruk*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(aluyruk.ToArray());
                        }

                        /*aladres*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(aladres.ToArray());
                        }

                        /*alilceadi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alilceadi.ToArray());
                        }

                        /*alpostakod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alpostakod.ToArray());
                        }

                        /*alilkod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alilkod.ToArray());
                        }

                        /*aliladi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(aliladi.ToArray());
                        }

                        /*altel*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(altel.ToArray());
                        }

                        /*alokhesno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alokhesno.ToArray());
                        }

                        /*alokepara*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alokepara.ToArray());
                        }

                        /*alokkartno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alokkartno.ToArray());
                        }

                        /*albankaad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(albankaad.ToArray());
                        }

                        /*albankakod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(albankakod.ToArray());
                        }

                        /*alsubead*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alsubead.ToArray());
                        }

                        /*aliban*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(aliban.ToArray());
                        }

                        /*alhesno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alhesno.ToArray());
                        }

                        /*alkredikartno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(alkredikartno.ToArray());
                        }

                        /*istar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(istar.ToArray());
                        }

                        /*odemetar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(odemetar.ToArray());
                        }

                        /*islemtutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(islemtutar.ToArray());
                        }

                        /*asiltutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(asiltutar.ToArray());
                        }

                        /*parabirim*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(parabirim.ToArray());
                        }

                        /*bruttutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(bruttutar.ToArray());
                        }

                        /*sirketmi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(sirketmi.ToArray());
                        }

                        /*sirketvkn*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(sirketvkn.ToArray());
                        }

                        /*sirketunvan*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(sirketunvan.ToArray());
                        }

                        /*bruttahtutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(bruttahtutar.ToArray());
                        }

                        /*kuraciklama*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(kuraciklama.ToArray());
                        }

                        /*musaciklama*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<string>())
                        {
                            objectIdWriter.WriteBatch(musaciklama.ToArray());
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
			string prefix = "KK002_SFP_";
			string date = DateTime.Now.ToString("yyyy_MM_dd");
			string counterString = GetCounter();

			string fileName = $"{prefix}{date}_{counterString}.parquet";

			return fileName;
		}
	}
}

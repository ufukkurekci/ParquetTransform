using Microsoft.Data.SqlClient;
using ParquetSharp;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Encoding = System.Text.Encoding;

namespace EK3.BinaryVersion
{
    public class EK3_ParquetOperation
    {
        public void GetParquetFile()
        {
            // Veritabanı bağlantı dizesi
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

            builder.DataSource = "172.25.84.24";
            builder.UserID = "quantra";
            builder.Password = "quantra2";
            builder.InitialCatalog = "EPara";
            builder.Encrypt = true;
            builder.TrustServerCertificate = true;


            // (EK3) Sanal-Fiziksel Pos Formu
            // KK002_SFP_2020_12_20_0001.parquet
            string sqlQuery = "EPara.dbo.GENERATE_PARQUET_EK3";

            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {

                connection.Open();

                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {

                    using (SqlDataReader reader = command.ExecuteReader())
                    {

                        List<byte> RecordType = new List<byte>();
                        List<byte> LocalRef = new List<byte>();
                        List<byte> İslemTuruKodu = new List<byte>();
                        List<byte> VergiKimlikNo = new List<byte>();
                        List<byte> TuzelKisilikUnvan = new List<byte>();
                        List<byte> gonposbankaad = new List<byte>();
                        List<byte> gonposbankakod = new List<byte>();
                        List<byte> gonuyelikno = new List<byte>();
                        List<byte> gonterno = new List<byte>();
                        List<byte> gonbankaad = new List<byte>();
                        List<byte> gonbankakod = new List<byte>();
                        List<byte> goniban = new List<byte>();
                        List<byte> gonhesno = new List<byte>();
                        List<byte> musterimi = new List<byte>();
                        List<byte> altkvkn = new List<byte>();
                        List<byte> altkunvan = new List<byte>();
                        List<byte> algkkimlikno = new List<byte>();
                        List<byte> algkad = new List<byte>();
                        List<byte> algksoyad = new List<byte>();
                        List<byte> alnosuzad = new List<byte>();
                        List<byte> alnosuzkimliktipi = new List<byte>();
                        List<byte> alnosuzkimlikno = new List<byte>();
                        List<byte> alnosuzulke = new List<byte>();
                        List<byte> alnosuziladi = new List<byte>();
                        List<byte> aluyruk = new List<byte>();
                        List<byte> aladres = new List<byte>();
                        List<byte> alilceadi = new List<byte>();
                        List<byte> alpostakod = new List<byte>();
                        List<byte> alilkod = new List<byte>();
                        List<byte> aliladi = new List<byte>();
                        List<byte> altel = new List<byte>();
                        List<byte> alokhesno = new List<byte>();
                        List<byte> alokepara = new List<byte>();
                        List<byte> alokkartno = new List<byte>();
                        List<byte> albankaad = new List<byte>();
                        List<byte> albankakod = new List<byte>();
                        List<byte> alsubead = new List<byte>();
                        List<byte> aliban = new List<byte>();
                        List<byte> alhesno = new List<byte>();
                        List<byte> alkredikartno = new List<byte>();
                        List<byte> istar = new List<byte>();
                        List<byte> odemetar = new List<byte>();
                        List<byte> nettutar = new List<byte>();
                        List<byte> islemtutar = new List<byte>();
                        List<byte> asiltutar = new List<byte>();
                        List<byte> parabirim = new List<byte>();
                        List<byte> bruttutar = new List<byte>();
                        List<byte> sirketmi = new List<byte>();
                        List<byte> sirketvkn = new List<byte>();
                        List<byte> sirketunvan = new List<byte>();
                        List<byte> bruttahtutar = new List<byte>();
                        List<byte> kuraciklama = new List<byte>();
                        List<byte> musaciklama = new List<byte>();
                        List<byte> kurumkod = new List<byte>();

                        var columns = new Column[]
                        {
                            new Column<byte>("recordtype"),
                            new Column<byte>("lref"),
                            new Column<byte>("islemturu"),
                            new Column<byte>("gonokvkn"),
                            new Column<byte>("gonokunvan"),
                            new Column<byte>("gonposbankaad"),
                            new Column<byte>("gonposbankakod"),
                            new Column<byte>("gonuyelikno"),
                            new Column<byte>("gonterno"),
                            new Column<byte>("gonbankaad"),
                            new Column<byte>("gonbankakod"),
                            new Column<byte>("goniban"),
                            new Column<byte>("gonhesno"),
                            new Column<byte>("musterimi"),
                            new Column<byte>("altkvkn"),
                            new Column<byte>("altkunvan"),
                            new Column<byte>("algkkimlikno"),
                            new Column<byte>("algkad"),
                            new Column<byte?>("algksoyad"),
                            new Column<byte>("alnosuzad"),
                            new Column<byte>("alnosuzkimliktipi"),
                            new Column<byte>("alnosuzkimlikno"),
                            new Column<byte>("alnosuzulke"),
                            new Column<byte>("alnosuziladi"),
                            new Column<byte>("aluyruk"),
                            new Column<byte>("aladres"),
                            new Column<byte>("alilceadi"),
                            new Column<byte>("alpostakod"),
                            new Column<byte>("alilkod"),
                            new Column<byte>("aliladi"),
                            new Column<byte>("altel"),
                            new Column<byte>("alokhesno"),
                            new Column<byte>("alokepara"),
                            new Column<byte>("alokkartno"),
                            new Column<byte>("albankaad"),
                            new Column<byte>("albankakod"),
                            new Column<byte>("alsubead"),
                            new Column<byte>("aliban"),
                            new Column<byte>("alhesno"),
                            new Column<byte>("alkredikartno"),
                            new Column<byte>("istar"),
                            new Column<byte>("odemetar"),
                            new Column<byte>("nettutar"),
                            new Column<byte>("islemtutar"),
                            new Column<byte>("asiltutar"),
                            new Column<byte>("parabirim"),
                            new Column<byte>("bruttutar"),
                            new Column<byte>("sirketmi"),
                            new Column<byte>("sirketvkn"),
                            new Column<byte>("sirketunvan"),
                            new Column<byte>("bruttahtutar"),
                            new Column<byte>("kuraciklama"),
                            new Column<byte>("musaciklama"),
                            new Column<byte>("kurumkod")

                        };
                        #region row_reader_area
                        while (reader.Read())
                        {
                            RecordType.AddRange(Encoding.UTF8.GetBytes(reader.GetString("recordtype")));
                            LocalRef.AddRange(Encoding.UTF8.GetBytes(reader.GetString("lref")));
                            İslemTuruKodu.AddRange(Encoding.UTF8.GetBytes(reader.GetString("islemturu")));
                            VergiKimlikNo.AddRange(Encoding.UTF8.GetBytes(reader.GetString("VergiKimlikNo")));
                            TuzelKisilikUnvan.AddRange(Encoding.UTF8.GetBytes(reader.GetString("gonokunvan")));
                            gonposbankaad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("gonposbankaad")));
                            gonposbankakod.AddRange(Encoding.UTF8.GetBytes(reader.GetString("gonposbankakod")));
                            gonuyelikno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("gonuyelikno")));
                            gonterno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("gonterno")));
                            gonbankaad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("gonbankaad")));
                            gonbankakod.AddRange(Encoding.UTF8.GetBytes(reader.GetString("gonbankakod")));
                            goniban.AddRange(Encoding.UTF8.GetBytes(reader.GetString("goniban")));
                            gonhesno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("gonhesno")));
                            musterimi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("musterimi")));
                            altkvkn.AddRange(Encoding.UTF8.GetBytes(reader.GetString("altkvkn")));
                            altkunvan.AddRange(Encoding.UTF8.GetBytes(reader.GetString("altkunvan")));
                            algkkimlikno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("algkkimlikno")));
                            algkad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("algkad")));
                            algksoyad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("algksoyad")));
                            alnosuzad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alnosuzad")));
                            alnosuzkimliktipi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alnosuzkimliktipi")));
                            alnosuzkimlikno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alnosuzkimlikno")));
                            alnosuzulke.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alnosuzulke")));
                            alnosuziladi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alnosuziladi")));
                            aluyruk.AddRange(Encoding.UTF8.GetBytes(reader.GetString("aluyruk")));
                            aladres.AddRange(Encoding.UTF8.GetBytes(reader.GetString("aladres")));
                            alilceadi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alilceadi")));
                            alpostakod.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alpostakod")));
                            alilkod.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alilkod")));
                            aliladi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("aliladi")));
                            altel.AddRange(Encoding.UTF8.GetBytes(reader.GetString("altel")));
                            alokhesno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alokhesno")));
                            alokepara.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alokepara")));
                            alokkartno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alokkartno")));
                            albankaad.AddRange(Encoding.UTF8.GetBytes(reader.GetString("albankaad")));
                            albankakod.AddRange(Encoding.UTF8.GetBytes(reader.GetString("albankakod")));
                            alsubead.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alsubead")));
                            aliban.AddRange(Encoding.UTF8.GetBytes(reader.GetString("aliban")));
                            alhesno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alhesno")));
                            alkredikartno.AddRange(Encoding.UTF8.GetBytes(reader.GetString("alkredikartno")));
                            istar.AddRange(Encoding.UTF8.GetBytes(reader.GetString("istar")));
                            odemetar.AddRange(Encoding.UTF8.GetBytes(reader.GetString("odemetar")));
                            nettutar.AddRange(Encoding.UTF8.GetBytes(reader.GetDecimal("nettutar").ToString()));
                            islemtutar.AddRange(Encoding.UTF8.GetBytes(reader.GetDecimal("islemtutar").ToString()));
                            asiltutar.AddRange(Encoding.UTF8.GetBytes(reader.GetDecimal("asiltutar").ToString()));
                            parabirim.AddRange(Encoding.UTF8.GetBytes(reader.GetString("parabirim")));
                            bruttutar.AddRange(Encoding.UTF8.GetBytes(reader.GetDecimal("bruttutar").ToString()));
                            sirketmi.AddRange(Encoding.UTF8.GetBytes(reader.GetString("sirketmi")));
                            sirketvkn.AddRange(Encoding.UTF8.GetBytes(reader.GetString("sirketvkn")));
                            sirketunvan.AddRange(Encoding.UTF8.GetBytes(reader.GetString("sirketunvan")));
                            bruttahtutar.AddRange(Encoding.UTF8.GetBytes(reader.GetDecimal("bruttahtutar").ToString()));
                            kuraciklama.AddRange(Encoding.UTF8.GetBytes(reader.GetString("kuraciklama")));
                            musaciklama.AddRange(Encoding.UTF8.GetBytes(reader.GetString("musaciklama")));
                            kurumkod.AddRange(Encoding.UTF8.GetBytes(reader.GetString("kurumkod")));


                        }
                        #endregion

                        using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "parquet", "KK002_SFP_2020_12_20_0001.parquet"), columns);
                        using var rowGroup = file.AppendRowGroup();
                        #region parquet_writer

                        /*RecordType*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(RecordType.ToArray());
                        }

                        /*LocalRef*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(LocalRef.ToArray());
                        }

                        /*İslemTuruKodu*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(İslemTuruKodu.ToArray());
                        }

                        /*VergiKimlikNo*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(VergiKimlikNo.ToArray());
                        }

                        /*TuzelKisilikUnvan*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(TuzelKisilikUnvan.ToArray());
                        }

                        /*gonposbankaad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(gonposbankaad.ToArray());
                        }

                        /*gonposbankakod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(gonposbankakod.ToArray());
                        }

                        /*gonuyelikno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(gonuyelikno.ToArray());
                        }

                        /*gonterno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(gonterno.ToArray());
                        }

                        /*gonbankaad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(gonbankaad.ToArray());
                        }

                        /*gonbankakod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(gonbankakod.ToArray());
                        }

                        /*goniban*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(goniban.ToArray());
                        }

                        /*gonhesno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(gonhesno.ToArray());
                        }

                        /*musterimi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(musterimi.ToArray());
                        }

                        /*altkvkn*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(altkvkn.ToArray());
                        }

                        /*altkunvan*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(altkunvan.ToArray());
                        }

                        /*algkkimlikno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(algkkimlikno.ToArray());
                        }

                        /*algkad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(algkad.ToArray());
                        }

                        /*algksoyad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(algksoyad.ToArray());
                        }

                        /*alnosuzad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alnosuzad.ToArray());
                        }

                        /*alnosuzkimliktipi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alnosuzkimliktipi.ToArray());
                        }

                        /*alnosuzkimlikno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alnosuzkimlikno.ToArray());
                        }

                        /*alnosuzulke*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alnosuzulke.ToArray());
                        }

                        /*alnosuziladi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alnosuziladi.ToArray());
                        }

                        /*aluyruk*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(aluyruk.ToArray());
                        }

                        /*aladres*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(aladres.ToArray());
                        }

                        /*alilceadi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alilceadi.ToArray());
                        }

                        /*alpostakod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alpostakod.ToArray());
                        }

                        /*alilkod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alilkod.ToArray());
                        }

                        /*aliladi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(aliladi.ToArray());
                        }

                        /*altel*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(altel.ToArray());
                        }

                        /*alokhesno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alokhesno.ToArray());
                        }

                        /*alokepara*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alokepara.ToArray());
                        }

                        /*alokkartno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alokkartno.ToArray());
                        }

                        /*albankaad*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(albankaad.ToArray());
                        }

                        /*albankakod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(albankakod.ToArray());
                        }

                        /*alsubead*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alsubead.ToArray());
                        }

                        /*aliban*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(aliban.ToArray());
                        }

                        /*alhesno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alhesno.ToArray());
                        }

                        /*alkredikartno*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(alkredikartno.ToArray());
                        }

                        /*istar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(istar.ToArray());
                        }

                        /*odemetar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(odemetar.ToArray());
                        }

                        /*nettutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(nettutar.ToArray());
                        }

                        /*islemtutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(islemtutar.ToArray());
                        }

                        /*asiltutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(asiltutar.ToArray());
                        }

                        /*parabirim*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(parabirim.ToArray());
                        }

                        /*bruttutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(bruttutar.ToArray());
                        }

                        /*sirketmi*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(sirketmi.ToArray());
                        }

                        /*sirketvkn*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(sirketvkn.ToArray());
                        }

                        /*sirketunvan*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(sirketunvan.ToArray());
                        }

                        /*bruttahtutar*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(bruttahtutar.ToArray());
                        }

                        /*kuraciklama*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(kuraciklama.ToArray());
                        }

                        /*musaciklama*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(musaciklama.ToArray());
                        }

                        /*kurumkod*/
                        using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
                        {
                            objectIdWriter.WriteBatch(kurumkod.ToArray());
                        }

                        #endregion
                        file.Close();

                    }
                }
            }
        }
    }
}

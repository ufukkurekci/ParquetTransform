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
		public static DateTime? today = null;
		public static DateTime? tomarrow = null;
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
            string sqlQuery = "GENERATE_PARQUET_EK3";



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

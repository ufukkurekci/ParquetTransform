using Microsoft.Data.SqlClient;
using ParquetSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EK7.NormalVersion
{
    public class ek7_validation
    {
        public void validation7()
        {
			#region list

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

			#endregion

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


            using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ek7_output", "KK002_OKKIB_2020_12_20_0001.parquet"), columns);

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

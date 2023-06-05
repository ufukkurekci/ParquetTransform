using Microsoft.Data.SqlClient;
using ParquetSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EK5.NormalVersion
{
    public class ek5_validation
    {
        public void validation5()
        {
			#region list


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

			#endregion

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

            using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ek5_output", "KK002_EPHPYCNI_2020_12_20_0001.parquet"), columns);

            using var rowGroup = file.AppendRowGroup();

			#region parquet_writer
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

			#endregion

			file.Close();




        }
    }
}

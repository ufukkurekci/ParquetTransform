using Microsoft.Data.SqlClient;
using ParquetSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EK7.BinaryVersion
{
    public class ek7_validation
    {
        public void validation7()
        {
            #region list

            List<byte> RecordType = new List<byte>();
            List<byte> LocalRef = new List<byte>();
            List<byte> İslemTuruKodu = new List<byte>();
            List<byte> VergiKimlikNo = new List<byte>();
            List<byte> TuzelKisilikUnvan = new List<byte>();
            List<byte> Ad = new List<byte>();
            List<byte> Soyad = new List<byte>();
            List<byte> KimlikTipi = new List<byte>();
            List<byte> KimlikNumarasi = new List<byte>();
            List<byte> KartNumarasi = new List<byte>();
            List<byte> Banktip = new List<byte>();
            List<byte> BankEftKod = new List<byte>();
            List<byte> BankAtmKod = new List<byte>();
            List<byte> IslemTarihi = new List<byte>();
            List<byte> islemTutar = new List<byte>();
            List<byte> AsilParaTutari = new List<byte>();
            List<byte> BirimISOCode = new List<byte>();
            List<byte> Komisyon = new List<byte>();
            List<byte> MusteriAciklama = new List<byte>();
            List<byte> KurumAciklama = new List<byte>();
            List<byte> kurumkod = new List<byte>();

            #endregion

            var columns = new Column[]
            {
                            new Column<byte>("recordtype"),
                            new Column<byte>("lref"),
                            new Column<byte>("islemturu"),
                            new Column<byte>("ksahtkvkn"),
                            new Column<byte>("ksahtkunvan"),
                            new Column<byte>("ksahgkad"),
                            new Column<byte>("ksahgksoyad"),
                            new Column<byte>("ksahgkkimliktipi"),
                            new Column<byte>("ksahgkkimlikno"),
                            new Column<byte>("ksahkartno"),
                            new Column<byte>("banktip"),
                            new Column<byte>("bankeftkod"),
                            new Column<byte>("bankatmkod"),
                            new Column<byte>("istar"),
                            new Column<byte>("islemtutar"),
                            new Column<byte>("asiltutar"),
                            new Column<byte>("parabirim"),
                            new Column<byte>("brutkomtut"),
                            new Column<byte>("musaciklama"),
                            new Column<byte>("kuraciklama"),
                            new Column<byte>("kurumkod")
            };


            using var file = new ParquetFileWriter(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "parquet", "KK002_OKKIB_2020_12_20_0001.parquet"), columns);

            using var rowGroup = file.AppendRowGroup();

            #region parquet_writer

            /*RecordType*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())  /*RecordType*/
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

            /*Ad*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(Ad.ToArray());
            }

            /*Soyad*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(Soyad.ToArray());
            }

            /*KimlikTipi*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(KimlikTipi.ToArray());
            }

            /*KimlikNumarasi*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(KimlikNumarasi.ToArray());
            }

            /*KartNumarasi*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(KartNumarasi.ToArray());
            }

            /*Banktip*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(Banktip.ToArray());
            }

            /*BankEftKod*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(BankEftKod.ToArray());
            }

            /*BankAtmKod*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(BankAtmKod.ToArray());
            }

            /*IslemTarihi*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(IslemTarihi.ToArray());
            }

            /*islemTutar*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(islemTutar.ToArray());
            }

            /*AsilParaTutari*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(AsilParaTutari.ToArray());
            }

            /*BirimISOCode*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(BirimISOCode.ToArray());
            }

            /*Komisyon*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(Komisyon.ToArray());
            }

            /*MusteriAciklama*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(MusteriAciklama.ToArray());
            }

            /*KurumAciklama*/
            using (var objectIdWriter = rowGroup.NextColumn().LogicalWriter<byte>())
            {
                objectIdWriter.WriteBatch(KurumAciklama.ToArray());
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

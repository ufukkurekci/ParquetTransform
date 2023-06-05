using Microsoft.Data.SqlClient;
using ParquetSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EK3.BinaryVersion
{
    public class ek3_validation
    {
        public void validation3()
        {



            // (EK3) Sanal-Fiziksel Pos Formu
            // KK002_SFP_2020_12_20_0001.parquet

            #region list
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

            #endregion
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
                            new Column<byte>("algksoyad"),
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
                            new Column<byte>("musaciklama")

                        };



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

            #endregion

            file.Close();




        }
    }
}

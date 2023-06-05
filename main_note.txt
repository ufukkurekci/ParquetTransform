
using System.Diagnostics;


Console.WriteLine("Ek3 için => 3");
Console.WriteLine("Ek4 için => 4");
Console.WriteLine("Ek5 için => 5");
Console.WriteLine("Ek7 için => 7");
Console.WriteLine("*******************************************************************************");
Console.WriteLine("Dosya tipini sayi olarak giriniz :");

string? input = Console.ReadLine();

switch (input)
{
	case "3":
		// Run Ek3
		Console.WriteLine("Ek3 tipi secildi SFP olusturulucak.");
		Process.Start("C:\\Users\\ukurekci\\source\\repos\\ParquetTransfromProject\\EK3\\bin\\Debug\\net7.0\\EK3.exe");
		break;
	case "4":
		// Run Ek4 // ok alındı
		Console.WriteLine("Ek4 tipi secildi EPKBB olusturulacak.");
		Process.Start("C:\\Users\\ukurekci\\source\\repos\\ParquetTransfromProject\\EK4\\bin\\Debug\\net7.0\\EK4.exe");
		break;
	case "5":
		// Run Ek5
		Console.WriteLine("Ek5 tipi secildi EPHPYCNI olusturulacak");
		Process.Start("C:\\Users\\ukurekci\\source\\repos\\ParquetTransfromProject\\EK5\\bin\\Debug\\net7.0\\EK5.exe");
		break;
	case "7":
		// Run Ek7  // ok alındı
		Console.WriteLine("Ek7 tipi secildi OKKIB olusturulacak");
		Process.Start("C:\\Users\\ukurekci\\source\\repos\\ParquetTransfromProject\\EK7\\bin\\Debug\\net7.0\\EK7.exe");
		break;
	default:
		Console.WriteLine("Hatalı secim !");
		break;
}

Console.ReadLine();

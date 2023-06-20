using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GlobalHelper
{
	public class SftpHelper
	{
		const string host = "212.174.188.170";
		const string username = "belbim";
		const string password = "A9F1JZh5XU";
		const string remoteDirectory = "/data/real/SFP/";


		public static void ConnectSftp(string localFilePath)
		{
			using (var client = new SftpClient(host, username, password))
			{
				client.Connect();

				if (client.IsConnected)
				{
					using (var fileStream = System.IO.File.OpenRead(localFilePath))
					{
						client.UploadFile(fileStream, remoteDirectory + System.IO.Path.GetFileName(localFilePath));
					}

					client.Disconnect();
				}
				else
				{
					Console.WriteLine("Unable to connect to the SFTP server.");
				}
			}
		}

		public static string LocalFilePath(string filename , string foldername)
		{
			return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, foldername, filename);
		}
	}
}

package org.globus.ftp.examples;

import org.globus.ftp.FTPClient;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import org.globus.ftp.DataSink;
import org.globus.ftp.DataSinkStream;
class FTPGet
{
	public static void main(String[] args)
	{
		FTPClient ftp = null;
		String hostname = "localhost";
		int port = 5555;
		String username = "anonymous";
		String password = "anonymous";
		
		try
		{
			ftp = new FTPClient(hostname, port);
		}
		catch(ServerException e)
		{
			System.out.println("Server exception: " + e.getMessage());
			System.exit(1);
		}
		// must be an IOException
		catch(Exception e)
		{
			System.out.println("error instantiating FTP client: " + e.toString());
			System.exit(1);
		}

		File localFile = new File("passwd");
		String remoteFile = "/etc/passwd";
		DataSink sink = null;
		
		try
		{
			sink = new DataSinkStream(new FileOutputStream(localFile));
		}
		catch(FileNotFoundException e)
		{
			System.out.println("could not access client destination: " + 
				e.toString());
			System.exit(1);
		}

		try
		{
			ftp.authorize(username, password);
		}
		catch(ServerException e)
		{
			System.out.println("Server exception authorizing: " + e.getMessage());
			System.exit(1);
		}
		// must be an IOException
		catch(Exception e)
		{
			System.out.println("error authorizing: " + e.toString());
			System.exit(1);
		}
		try
		{
			// third parameter is an optional MarkerListener
			ftp.get(remoteFile, sink, null);
		}
		catch(ServerException e)
		{
			System.out.println("Server exception getting file: " + e.getMessage());
			System.exit(1);
		}
		catch(ClientException e)
		{
			System.out.println("Client exception getting file: " + e.getMessage());
			System.exit(1);
		}
		// must be an IOException
		catch(Exception e)
		{
			System.out.println("error getting file: " + e.toString());
			System.exit(1);
		}
		try
		{
			ftp.close();
		}
		catch(Exception e)
		{
		}
	}
}

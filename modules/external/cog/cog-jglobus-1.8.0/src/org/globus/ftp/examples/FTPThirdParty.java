package org.globus.ftp.examples;

import org.globus.ftp.FTPClient;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
class FTPThirdParty
{
	public static void main(String[] args)
	{
		FTPClient ftp1 = null;
		FTPClient ftp2 = null;

		// first host
		String hostname1 = "localhost";
		int port1 = 5555;
		String username1 = "anonymous";
		String password1 = "anonymous";

		// second host
		String hostname2 = "localhost";
		int port2 = 5556;
		String username2 = "anonymous";
		String password2 = "anonymous";

		String remoteSource = "/etc/passwd";
		String remoteDest = "/tmp/mypasswd";
		boolean append = false;
		
		try
		{
			ftp1 = new FTPClient(hostname1, port1);
			ftp2 = new FTPClient(hostname2, port2);
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

		try
		{
			ftp1.authorize(username1, password1);
			ftp2.authorize(username2, password2);
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
			// fifth parameter is an optional MarkerListener
			ftp1.transfer(remoteSource, ftp2, remoteDest, append, null);
		}
		catch(ServerException e)
		{
			System.out.println("Server exception transferring file: " + 
				e.getMessage());
			System.exit(1);
		}
		catch(ClientException e)
		{
			System.out.println("Client exception transferring file: " + 
				e.getMessage());
			System.exit(1);
		}
		// must be an IOException
		catch(Exception e)
		{
			System.out.println("error transferring file: " + e.toString());
			System.exit(1);
		}
		try
		{
			ftp1.close();
			ftp2.close();
		}
		catch(Exception e)
		{
		}
	}
}

//
// @(#) $Id: FTPTransactionLog.java,v 1.6 2007-08-03 20:20:00 timur Exp $
//
// $Log: not supported by cvs2svn $
// Revision 1.5  2007/08/03 15:46:01  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes etc, per findbug recommendations
//
// Revision 1.4  2004/09/18 15:01:14  timur
// check that were previously closed in close
//
// Revision 1.3  2004/09/08 21:25:43  timur
// remote gsiftp transfer manager will now use ftp logger too, fixed ftp door logging problem
//
// Revision 1.2  2003/06/04 22:15:02  cvs
// fixed logging to produce more readable reports
//
// Revision 1.1  2002/02/19 20:30:04  cvs
// Added new files for fermilab k5 authentication
//
// Revision 1.2  2001/09/21 19:22:55  ivm
// Tested, working
//
// Revision 1.1  2001/09/20 02:45:53  ivm
// Not working version yet
//
//

package diskCacheV111.doors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

public class	FTPTransactionLog
{
        private static final Logger _log =
            LoggerFactory.getLogger(FTPTransactionLog.class);

	private FileWriter LWriter;
	private File LogFilePath;
	private final String root;
	private String tid;
	private boolean GotMiddle;

	public FTPTransactionLog(String root, String tid)
	{
		this(root);
		this.tid = tid;
	}

	public FTPTransactionLog(String root)
	{
		long time = System.currentTimeMillis();
		this.root = root;
		this.tid = "" + time;
		LWriter = null;
		GotMiddle = false;
	}

	@Override
        public synchronized void finalize() throws Throwable
        {
		error("Transaction abandoned");
                super.finalize();
	}

	private synchronized void addLine(String line)
	{
		if( LWriter == null ) {
                    return;         // it's closed already
                }
		try
		{
			long time = System.currentTimeMillis() / 1000;
			LWriter.write(time + " " + line + "\n");
			LWriter.flush();
		}
		catch(IOException e)
		{
                    _log.warn(e.toString(), e);
                }
	}

	public synchronized void begin(String user, String ftp_type, String rw,
		String path, InetAddress addr)
	{
		if( root == null ) {
                    return;
                }
		try
		{
			//System.out.println("TLog.begin... Tid="+Tid+" Root="+Root);
			File rootDir = new File(root);
			if ( !rootDir.exists() )
			{
				//System.out.println("Root <"+Root+"> does not exist");
				throw new IOException("Log root directory "+root+" not found");
			}
			String dirname = user;
			dirname = dirname.replaceAll("/", ":");

			File userDir = new File(rootDir, dirname);
			if ( !userDir.exists() ) {
                            userDir.mkdir();
                        }

			LogFilePath = new File(userDir, tid + ".tlog");
			LWriter = new FileWriter(LogFilePath);
			File userNameFile = new File(userDir, "username");
			FileWriter userNameFileWriter = new FileWriter(userNameFile);
			userNameFileWriter.write(user);
			userNameFileWriter.close();
			try {
			//InetAddress addr = InetAddress.getByName(ipaddr.substring(1));
			String line = user + " \"" +
				ftp_type + "\" " +
				rw + " " +
				path + " " +
				addr.getCanonicalHostName();
			addLine(line);

			}
			catch( Exception ex) {
                            _log.warn(ex.toString(), ex);
			    addLine("Unable to process IP for transfer");
			}
		}
		catch( IOException e )
		{
                    _log.warn(e.toString(), e);
                }
	}

	public synchronized void middle(long size)
	{
		addLine(""+size);
		GotMiddle = true;
	}

	public synchronized void error(String status)
	{
		if( !GotMiddle ) {
                    middle(0);
                }
		addLine("ERROR " + status);
		close();
	}

	public synchronized void success()
	{
		if( !GotMiddle ) {
                    middle(0);
                }
		addLine("OK");
		close();
	}

	private synchronized void close()
	{
		if(LWriter == null)
                {
                        return;
                }
		try{
			LWriter.close();	}
		catch(Exception e)	{	_log.warn(e.toString(), e);/* ignore */	}
		LWriter = null;
	}
}

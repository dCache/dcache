//
// @(#) $Id: FTPTransactionLog.java,v 1.4.24.1 2007-09-14 22:37:12 aik Exp $
//
// $Log: not supported by cvs2svn $
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

import java.util.*;
import java.io.*;
import java.net.InetAddress;
import dmg.cells.nucleus.CellAdapter;


public class	FTPTransactionLog
{
//	private FileWriter LWriter = null;
	private File LogFilePath = null;
	private String root;
	private String tid;
	private boolean GotMiddle;
        CellAdapter cell;

	public FTPTransactionLog(String root, String tid, CellAdapter cell)
	{
		this(root,cell);
		this.tid = tid;
	}

	public FTPTransactionLog(String root,CellAdapter cell)
	{
		long time = System.currentTimeMillis();
		this.root = root;
		this.tid = "" + time;
		//LWriter = null;
		GotMiddle = false;
                this.cell = cell;
	}

        private void esay(Throwable t)
        {
            if(cell != null){
                cell.esay(t);
            }
        }

	private void addLine(String line)
	{
            // There are only few lines per log, keep it closed, open each time
            FileWriter LWriter;

            if ( LogFilePath == null )
                return;

            try {
                LWriter = new FileWriter(LogFilePath, true);
            } catch(IOException ioe) {
                esay(ioe);
                return;
            }

            try {
                long time = System.currentTimeMillis() / 1000;
                LWriter.write(time + " " + line + "\n");
                LWriter.flush();
            } catch(IOException e) {
                esay(e);
            } finally {
                try {
                    LWriter.close();
                } catch (IOException ex) { ; }
            }
	}

	public void begin(String user, String ftp_type, String rw,
		String path, InetAddress addr)
	{
		if( root == null )
			return;
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
			dirname.replaceAll("/", ":");

			File userDir = new File(rootDir, dirname);
			if ( !userDir.exists() )
				userDir.mkdir();

			LogFilePath = new File(userDir, tid + ".tlog");
//			LWriter = new FileWriter(LogFilePath);

			File userNameFile = new File(userDir, "username");
			FileWriter userNameFileWriter = new FileWriter(userNameFile);
			userNameFileWriter.write(user);

			try {
                            //InetAddress addr = InetAddress.getByName(ipaddr.substring(1));
                            String line = user + " \"" +
                                          ftp_type + "\" " +
                                          rw + " " +
                                          path + " " +
                                          addr.getCanonicalHostName();
                            addLine(line);

			} catch( Exception ex) {
                            esay(ex);
			    addLine("Unable to process IP for transfer");
			}
		}
		catch( IOException e )
		{
                    esay(e);
                }
	}

	public void middle(long size)
	{
		addLine(""+size);
		GotMiddle = true;
	}

	public void error(String status)
	{
		if( !GotMiddle )	middle(0);
		addLine("ERROR " + status);
		close();
	}

	public void success()
	{
		if( !GotMiddle )	middle(0);
		addLine("OK");
		close();
	}

	private synchronized void close()
	{
	}
}

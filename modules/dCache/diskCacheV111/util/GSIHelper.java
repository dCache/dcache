/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
*/


package diskCacheV111.util;
import dmg.cells.nucleus.CellAdapter;
import java.util.*;
import java.io.*;
import diskCacheV111.util.Base64;

//import java.net.*;

public class GSIHelper
{
	private Process Helper;
	private InputStream StdErr, StdOut;
	private OutputStream StdIn;
	private String ClientName = null;
	private BufferedReader OutReader;
	private String DataToSend = null;
	private String Error = null;
    private CellAdapter adapter = null;

	public GSIHelper(String executable,CellAdapter adapter)
		throws IOException
    {
        this(executable);
        this.adapter = adapter;
    }

    public GSIHelper(String executable)
		throws IOException
	{
		Helper = Runtime.getRuntime().exec(executable);
		StdErr = Helper.getErrorStream();
		StdIn  = Helper.getOutputStream();
		StdOut = Helper.getInputStream();
		OutReader = new BufferedReader(new InputStreamReader(StdOut));
	}

	private String sendRecv(String msg)
		throws IOException
	{
		StdIn.write(msg.getBytes());
		StdIn.write('\n');
		StdIn.flush();
		say("Sent: <" + msg + ">");
		String answer = OutReader.readLine();
		if( answer == null )
			answer = "";
		answer = answer.trim();
		say("Rcvd: <" + answer + ">");
		return answer;
	}


	public int handleAuthData(String data)
	{
		say("handleAuthData(" + data + ")");
		try
		{
			DataToSend = null;

			String answer = sendRecv("AUTH " + data);

			if( answer.startsWith("DONE") )
			{
				answer = answer.substring(4).trim();
				int endOfName = answer.indexOf(">");
				ClientName = answer.substring(1, endOfName);
				DataToSend = answer.substring(endOfName+1).trim();
				return 1;
			}
			else if( answer.startsWith("ERR") )
			{
				Error = answer.substring(3).trim();
				return -1;
			}
			else if( answer.startsWith("CONT") )
			{
				DataToSend = answer.substring(4).trim();
				return 0;
			}
			else
			{
				Error = "Unrecognized answer <" + answer + ">";
				return -1;
			}
		}
		catch( Exception e )
		{
			Error = e.toString();
			return -1;
		}
	}

	public String makeMIC(String msg)
		throws IOException
	{
		// returns base64 encoded MIC string
		String answer = sendRecv("MIC " +
			Base64.byteArrayToBase64(msg.getBytes()));
		// cut out OK
		int inx = answer.indexOf(" ");
		return answer.substring(inx).trim();
	}

	public String makeENC(String msg)
		throws IOException
	{
		// returns base64 encoded MIC string
		String answer = sendRecv("ENC " +
			Base64.byteArrayToBase64(msg.getBytes()));
		// cut out OK
		int inx = answer.indexOf(" ");
		return answer.substring(inx).trim();
	}

	public byte[] unMIC(String msg)
		throws IOException
	{
		// returns unwrapped byte array
		String answer = sendRecv("UNMIC " + msg);
		int inx = answer.indexOf(" ");
		return Base64.base64ToByteArray(answer.substring(inx).trim());
	}

	public void quit()
	{
		String quit = "QUIT\n";
		try
		{
			StdIn.write(quit.getBytes());
			StdErr.close();
			StdIn.close();
			StdOut.close();
		}
		catch ( Exception e )
		{	/* ignore */ ;	}
		Helper.destroy();
	}

	public String dataToSend()
	{
		return DataToSend;
	}

	public String clientName()
	{
		return ClientName;
	}

	public String error()
	{
		return Error;
	}

    public void say(String s)
    {
        if(adapter != null)
        {
            // comment saying for now
            // can be uncommented if debugging is needed
            //adapter.say("GSIHelper : "+s);
        }
    }

    public void esay(String e)
    {
        if(adapter !=null)
        {
            adapter.esay("GSIHelper : "+e);
        }
    }

    public void esay(Throwable t)
    {
        if(adapter !=null)
        {
            adapter.esay("GSIHelper throwable: ");
            adapter.esay(t);
        }
    }



}


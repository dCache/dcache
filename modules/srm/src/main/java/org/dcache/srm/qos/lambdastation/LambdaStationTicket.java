//$Id: LambdaStationTicket.java,v 1.3 2006/06/15 20:49:50 moibenko Exp $

package org.dcache.srm.qos.lambdastation;

//import java.io.PrintWriter;

import java.io.StringWriter;
import java.util.StringTokenizer;

import org.dcache.srm.qos.QOSTicket;
import org.dcache.srm.util.ShellCommandExecuter;

// Lambda Station Ticket

public class LambdaStationTicket implements QOSTicket {

    // The foolowing is taken from openSvcTicket input
    // parameters list

    public String SrcSite;    //src site ID: "Fermilab"
    public String SrcClient;  // src client ID "CMS-SRM"
    public String SrcIP;      // src IP list in CIDR format: "131.225.207.0/25,
                                   // 131.225.207.133, 131.225.207,134"
    public String SrcPort;    // list of ports: "tcp eq 25, tcp range 5000-6000, udp le 3200"

    public String DstSite;    //src site ID: "Caltech"
    public String DstClient;  // src client ID "CMS-SRM"
    public String DstIP;      // src IP list in CIDR format: "131.225.207.0/25,
                                   // 131.225.207.133, 131.225.207,134"
    public String DstPort;    // list of ports: "tcp eq 25, tcp range 5000-6000, udp le 3200"

    public String LocalPath;  // StarLight10G
    public String RemotePath; //
    public String OutBW;      // Requested outbound bandwidth: "5G", "500M"
    public String InBW;       // Requested  inbound bandwidth: "5G", "500M"
    public String DSCPrqOut;   // request for outbound DSCP: "YES, NO, DESIRABLE"
    public String DSCPrqIn;   // request for inbound DSCP: "YES, NO, DESIRABLE"
    public String BoardTime;
    public String StartTime;
    public String EndTime;
    public String TravelTime; // for how long is path reserved (sec)

    public int DSCPin;
    public int DSCPout;

    // The following may be needed
    private String LambdaStationId;
    private String credentialSubject;

    private int localTicketID; // local ticket id returned by Lambda Station
    private int remoteTicketID; // remote ticket id returned by Lambda Station
    private long actualEndTime; // actual end time returned by Lambda Station
    public boolean srcEnabled = true;
    public boolean dstEnabled = true;

    public Long bytes;// Jay


    public LambdaStationTicket(){

    }
    public LambdaStationTicket(String credentialSubject){
        this.credentialSubject = credentialSubject;
    }


    public LambdaStationTicket(
            String credentialSubject,
            long bytes,
            String srcURL,
            String srcClient,
            String srcPorts,
            String dstURL,
            String dstClient,
            String dstPorts,
            LambdaStationMap map){
        this.credentialSubject = credentialSubject;
        SrcSite = map.getName(srcURL);
        SrcClient = srcClient;
        SrcPort = srcPorts;
        DstSite = map.getName(dstURL);
        DstClient = dstClient;
        DstPort = dstPorts;
        srcEnabled = map.enabled(srcURL);
        dstEnabled = map.enabled(dstURL);

    }

    public LambdaStationTicket(
            String credentialSubject,
            String srcURL,
            String srcClient,
            String dstURL,
            String dstClient,
            LambdaStationMap map){

        this.credentialSubject = credentialSubject;
        SrcSite = map.getName(srcURL);
        SrcClient = srcClient;
        DstSite = map.getName(dstURL);
        DstClient = dstClient;
        srcEnabled = map.enabled(srcURL);
        dstEnabled = map.enabled(dstURL);
   }
    public LambdaStationTicket(
            String credentialSubject,
            String srcURL,
            String srcClient,
            String dstURL,
            String dstClient,
	    int TravelTime,
            LambdaStationMap map){

        this.credentialSubject = credentialSubject;
        SrcSite = map.getName(srcURL);
        SrcClient = srcClient;
        DstSite = map.getName(dstURL);
        DstClient = dstClient;
	this.TravelTime = Integer.toString(TravelTime);
        srcEnabled = map.enabled(srcURL);
        dstEnabled = map.enabled(dstURL);
   }


    public void OpenTicket(String script){
        /* This command is specialized to use ost2
         All options are optional to overwrite defaults
 Options are:
     --statusWait       - how long to show status of created ticket (in secs),
     --srcSite          - identifier of source site
     --srcClient        - identifer of source client
     --dstSite          - identifier of destination site
     --dstClient        - identifier of destination
     --boardTime        - YYYYMMDDHHMMSS
     --travelTime       - for how long request path (in secs)
     --localPath        - specify an alternative path
     --remotePath       - specify an alternative remote path
     --srcIP            - a comma separate list of srcIPs nnn.nnn.nnn.nnn/mask
     --dstIP            - a comma separate list of dstIPs nnn.nnn.nnn.nnn/mask
     --verbose          - show more diagnostics
     --help             - this message
         */
        String c;
        // first check for -Dlambda
        if (System.getProperty("lambda") != null) {
            c = System.getProperty("lambda");
        }
        // Then check configuration
        else {
            if (script != null) {
                c = script;
            }
            else {
                say("AM: NO LS COMMAND");
                return;
            }

        }
        if (!(srcEnabled&dstEnabled)) {
            return;
        }
        String[] cmd = new String[2];
	cmd[0] = c;
        String arguments = "";
        if (SrcSite != null) {
            arguments = (arguments+"--srcSite "+SrcSite+" ");
        }
        if (SrcClient != null) {
            //arguments = (arguments+"--srcClient "+SrcClient+" ");
        }
        if (DstSite != null) {
            arguments = (arguments+"--dstSite "+DstSite+" ");
        }
        if (DstClient != null) {
            //arguments = (arguments+"--dstClient "+DstClient+" ");
        }
	if (TravelTime != null) {
	        arguments = (arguments+"--travelTime "+TravelTime+" ");
	}

        // Add more if needed
	cmd[1] = "'"+arguments+"'";
	say("ARGS="+arguments);
        StringWriter shell_out = new StringWriter();
        StringWriter shell_err = new StringWriter();

        int return_code = ShellCommandExecuter.execute(cmd,shell_out, shell_err);
	// Do not check return code
        //say("lambda output:");
        //say(shell_out.getBuffer().toString());
	//say("lambda error output:");
	//say(shell_err.getBuffer().toString());
	//System.out.println("lambda stdout:"+shell_out.getBuffer().toString());
	StringTokenizer responseTokenizer
	    = new StringTokenizer(shell_out.getBuffer().toString(), ":");
	//say("Tokens "+responseTokenizer.countTokens());
	while ( responseTokenizer.hasMoreElements() ) {
	    String tok = (String) responseTokenizer.nextElement();
	    if (tok.equals("OK")) {
		// skip "new"
		tok = (String) responseTokenizer.nextElement();
		// next token is a return value
		tok = (String) responseTokenizer.nextElement();
		StringTokenizer valTokenizer =
		    new StringTokenizer(tok, ",");
		String tok1 = (String) valTokenizer.nextElement();
		if (tok1 != null) {
		    localTicketID = Integer.parseInt(tok1);
		}
		else {
		    break;
		}
		tok1 = (String) valTokenizer.nextElement();
		if (tok1 != null) {
		    remoteTicketID = Integer.parseInt(tok1);
		}
		else {
		    break;
		}
		tok1 = (String) valTokenizer.nextElement();
		tok1 = tok1.trim();
		if (tok1 != null) {
		    actualEndTime = Long.parseLong(tok1);
		}
		break;
	    }
	    else if (tok.equals("ERROR")) {
		say("Lambda Station returned Error: "+shell_out.getBuffer().toString());
		break;
	    }

        }
    }

    public String getLambdaStationId(){
         return LambdaStationId;
    }
    public void setLambdaStationId(String lambdaStationId){
        this.LambdaStationId = lambdaStationId;
     }

    public String toString(){

        return ("SrcSite="+SrcSite+" SrcClient="+SrcClient+" SrcIP="+SrcIP+
                " SrcPort="+SrcPort+" DstSite="+DstSite+" DstClient="+DstClient+
                " DstIP="+DstIP+" DstPort="+DstPort+" LocalPath="+LocalPath+
                " RemotePath="+RemotePath+" OutBW="+OutBW+" InBW="+InBW+
                " DSCPrqOut="+DSCPrqOut+" DSCPrqIn="+DSCPrqIn+" BoardTime="+BoardTime+" TravelTime="+TravelTime+
                " StartTime="+StartTime+" EndTime="+EndTime+" DSCPin="+DSCPin+" DSCPout="+DSCPout+
		" credentialSubject="+credentialSubject+" localTicketID="+localTicketID+
		" remoteTicketID="+remoteTicketID+" actualEndTime="+actualEndTime);

    }

    public void say(String words) {
        System.out.println("LS_Ticket: "+words);
    }


    /**
     *
     * @return credentialSubject
     */
    public String getCredentialSubject() {
        return credentialSubject;
    }
    public int getLocalTicketID() {
        return localTicketID;
    }
    public void setLocalTicketID(int ID) {
        localTicketID = ID;
    }
    public int getRemoteTicketID() {
        return remoteTicketID;
    }
    public void setRemoteTicketID(int ID) {
        localTicketID = ID;
    }
    public long getActualEndTime() {
        return actualEndTime;
    }
    public boolean srcEnabled() {
	return this.srcEnabled;
    }
    public boolean dstEnabled() {
	return this.dstEnabled;
    }
    public long getStartTime() {
	long l = 0;
	if (this.StartTime != null) {
	    l = Long.parseLong(this.StartTime.trim());
	}
	return l;
    }

}


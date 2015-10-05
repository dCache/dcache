/*
 * Report.java
 *
 * Created on April 22, 2005, 3:17 PM
 */

package gov.fnal.srm.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

/**
 *
 * @author  timur
 */
public class Report {
    // return code constats
    public static final int OK_RC=0;
    public static final int ERROR_RC=1;
    public static final int FILE_EXISTS_RC=2;
    public static final int PERMISSION_RC=3;
    public static final int MAX_RC_VALUE=3;
    //
    public static final String INITIAL_ERROR=
        "copy did not complete or status unknown";
    // source urls
    URI from[];
    //destination urls
    URI to[];
    //return codes
    int rc[];
    //errors
    String error[];
    // number of url pairs
    int length;
    //
    private File reportFile;

    /** Creates a new instance of Report */
    public Report(URI from[], URI to[], String reportFileName)  {
        if(from == null || to == null) {
            throw new NullPointerException(
            "from url array and to url array should not be null");
        }
        if(reportFileName != null) {
            reportFile = new File(reportFileName);
            try {
                if(reportFile.exists() ) {
                    if(!reportFile.canWrite()) {
                        throw new IllegalArgumentException(
                                " can not write into report file : "+reportFileName);
                    }
                }

                else if(!reportFile.createNewFile()) {
                    throw new IllegalArgumentException(
                            " can not write into report file : "+reportFileName);
                }
            } catch ( IOException ioe) {
                throw new IllegalArgumentException(
                        " can not write into report file : "+reportFileName+
                        " : "+ioe);
            }
        }
        this.from = from;
        this.to = to;
        length = from.length;
        if(to.length != length) {
            throw new IllegalArgumentException(
            "legths of from and to archives should be the same");
        }
        rc = new int[length];
        error = new String[length];
        for(int i = 0; i <length; ++i) {
            rc[i] =ERROR_RC;
            error[i]=INITIAL_ERROR;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i <length; ++i) {
            sb.append(from[i].toASCIIString());
            sb.append(' ');
            sb.append(to[i].toASCIIString());
            sb.append(' ');
            sb.append(rc[i]);
            if(rc[i] != 0) {
                sb.append(' ');
                sb.append(error[i].replace('\n',' '));
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    public void setStatusBySourceUrl(URI srcURL,int returnCode,String errorDscr){
        if(!isValidRC(returnCode)) {
            throw new IllegalArgumentException("illegal return code value : "+returnCode);
        }
        for(int i = 0; i <length; ++i) {
            if(srcURL.equals(from[i])) {
                rc[i] = returnCode;
                if(returnCode != 0) {
                    error[i] = errorDscr;
                }
            }
        }
    }

    public void setStatusByDestinationUrl(URI dstURL,int returnCode,String errorDscr){
        if(!isValidRC(returnCode)) {
            throw new IllegalArgumentException("illegal return code value : "+returnCode);
        }
        for(int i = 0; i <length; ++i) {
            if(dstURL.equals(to[i])){
                rc[i] = returnCode;
                if(returnCode != 0) {
                    error[i] = errorDscr;
                }
                return;
            }
        }
        throw new IllegalArgumentException("record for dest=" + dstURL + " not found");
    }

    public void setStatusBySourceDestinationUrl(URI srcURL, URI dstURL,int returnCode,String errorDscr){
        if(!isValidRC(returnCode)) {
            throw new IllegalArgumentException("illegal return code value : "+returnCode);
        }
        for(int i = 0; i <length; ++i) {
            if(dstURL.equals(to[i]) && srcURL.equals(from[i])) {
                rc[i] = returnCode;
                if(returnCode != 0) {
                    error[i] = errorDscr;
                }
                return;
            }
        }
        throw new IllegalArgumentException("record for source="+
                srcURL+" and dest="+dstURL+" not found");
    }

    public boolean isValidRC(int rc) {
        return rc >= 0 && rc <= MAX_RC_VALUE;
    }

    public void dumpReport() {
        if (reportFile != null) {
            try (FileWriter fw = new FileWriter(reportFile)) {
                fw.write(toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void reportErrors(PrintStream out) {
        for (int i = 0; i < length; ++i) {
            if (rc [i] != 0) {
                out.append(from[i].toASCIIString());
                if (to[i] != from[i]) {
                    out.append(" -> ");
                    out.append(to[i].toASCIIString());
                }
                out.append(": ");
                out.append(error[i].replace('\n', ' '));
            }
            out.println();
        }
    }

    public boolean everythingAllRight(){
        for (int returnCode : rc) {
            if (returnCode != OK_RC) {
                return false;
            }
        }
        return true;
    }
}

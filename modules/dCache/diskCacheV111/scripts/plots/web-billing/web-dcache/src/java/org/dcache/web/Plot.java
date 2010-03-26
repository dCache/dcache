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
package org.dcache.web;

import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;

/**
 * @author V. Podstavkov
 *
 */
public class Plot implements PlotSpecifier {

    /*
     * 
     * @param title
     * @param dataSet
     */
/*    
    public Plot(String title, DataSrc[] dataSet) 
    throws SQLException, IOException
    {
        this.plotTitle = title;                    // Store plot title 
        this.dataset = dataSet;                    // Save dataset
        String fname = "/tmp/plot-"+System.currentTimeMillis();

        for (int i = 0; i < dataset.length; i++) { // Store datasets into the files
            dataset[i].storeSet(fname+"."+i);
        }
    }
*/
    
    /**
     * Prepares the plot data for given sources 
     * @param conn
     * @param plotLabel
     * @param plotTitle 
     * @param dataSrc
     * @throws SQLException
     * @throws IOException
     */
    public Plot(Connection conn, String plotLabel, String plotTitle, String[][] dataSrc) 
    throws SQLException, IOException 
    {
        this.plotTitle = plotTitle;
        this.plotLabel = plotLabel;
        this.dataSources = new Hashtable<String,DataSrc>();
        this.dataSrcRefs = new ArrayList<String>();

        //
        for (int i = 0; i < dataSrc.length; i++) {
            String[] ds = dataSrc[i];                                       // Get the table name and SQL query string
            DataSrc dsrc = new DataSrc(conn, ds[0], ds[1]);                 // ds[0] contains table name, ds[1] contains query
            dsrc.store("/tmp/"+plotLabel+"."+i);                            // Save the result into the file
            dataSources.put(ds[0], dsrc);                                   // Store datasources in the dictionary
        }
    }

    /* (non-Javadoc)
     * @see PlotSpecifier#getTitle()
     */
    public String getTitle() {
        return plotTitle;
    }

//    /* (non-Javadoc)
//     * @see PlotSpecifier#getPNG()
//     */
//    public String getPNG() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    /* (non-Javadoc)
//     * @see PlotSpecifier#getPS()
//     */
//    public String getPS() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    /* (non-Javadoc)
//     * @see PlotSpecifier#getHTML()
//     */
//    public String getHTML() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    /* (non-Javadoc)
//     * @see PlotSpecifier#getText()
//     */
//    public String getText() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
    /* (non-Javadoc)
     * @see PlotSpecifier#getFileName()
     */
    public String getFileName() {
        return fileName;
    }

//    /* (non-Javadoc)
//     * @see PlotSpecifier#getThumbnail()
//     */
//    public String getThumbnail() {
//        // TODO Auto-generated method stub
//        return null;
//    }
    
    public final void register() {
        PlotStorage.register(this.plotLabel, this) ;
    }
    
    public void buildPlot(final String filename, PlotBuilder plotBuilder, Object hlp) 
    throws IOException
    {
        this.fileName = filename;
        plotBuilder.buildPlot(filename, this, hlp);
    }
    
    /**
     * Sends the file to the browser
     * @param filename
     * @throws IOException
     */
    public void sendPlot(final OutputStream bos)
    throws IOException
    {
        File file = new File(fileName);
        if (file.exists()) {
//            if (filename.indexOf(".jpg") > 0 || filename.indexOf(".jpeg") > 0) {
//                res.setContentType("image/jpeg");
//            } else if (filename.indexOf(".png") > 0) {
//                res.setContentType("image/png");
//            } else if (filename.indexOf(".ps") > 0 || filename.indexOf(".eps") > 0) {
//                res.setContentType("application/postscript");
//            }
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
//          BufferedOutputStream bos = new BufferedOutputStream(res.getOutputStream());
            byte[] input = new byte[1024];
            boolean eof = false;
            while (!eof) {
                int length = bis.read(input);
                if (length == -1) {
                    eof = true;
                } else {
                    bos.write(input,0,length);
                }
            }
//            bos.flush();
            bis.close(); bis=null;
//            bos.close(); bos=null;
        } else {
            throw new IOException("File not found: "+file.getAbsolutePath());
        }
    }

    /**
     * 
     * @return the number of datasets for the plot, not the number of datasources!
     */
    public int getDsLen() {
        return dataSrcRefs.size();
    }

    public DataSrc getDataSrc(int i) {
        if (i < dataSrcRefs.size())
            return dataSources.get(dataSrcRefs.get(i));
        return null;
    }

    public String getDataSrcFilename(int i) {
        if (i < dataSrcRefs.size())
            return getDataSrc(i).getFileName();
        return "";
    }

    public Integer getDSColNumber(int i, String name) {
        return getDataSrc(i).getColNumber(name);
    }

    public void addDataSet(String name) {
        dataSrcRefs.add(name);
    }
    
    public String getDataSetName(int i) {
        return dataSrcRefs.get(i);
    }
    
//    public String getPlotFormat() {
//        return plotFmt;
//    }
    
    private String plotTitle; 
    private String plotLabel; 
    private String fileName; 
    private Hashtable<String,DataSrc> dataSources;
    private String plotFmt;
    private ArrayList<String> dataSrcRefs;
}

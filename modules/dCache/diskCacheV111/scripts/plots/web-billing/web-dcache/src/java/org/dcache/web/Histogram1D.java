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

/**
 * Simple time series histogram
 */

import java.sql.*;
//import java.io.*;
//import java.io.File;
//import java.util.Date;
//import java.util.Calendar;


public class Histogram1D
{
    public Histogram1D(String name, Timestamp start, Timestamp end)
    {
        long stime = start.getTime();
        long etime = end.getTime();

        int nDays = (int)((float)(etime-stime)/1000.0/3600.0/24.0+0.5);

        if (nDays < 3) {
            binWidth = 1000l*1800l;
            nBins = nDays * 48;
        } else if (nDays >= 3 && nDays <=7) {
            binWidth = 1000l*3600l;
            nBins = nDays * 24;
        } else {
            binWidth = 1000l*3600l*24l;
            nBins = nDays;
        }

        hStart = stime;
        hEnd = stime + nBins*binWidth;
        entries = 0;
        total = 0.0;
        hBody = new double[nBins+2];
        for (int i = 0; i < hBody.length; i++) {
            hBody[i] = 0.0;
        }
        hName = new String(name);
    }
    /*
     public Histogram1D(String name, int start, int end, int nbins)
     {
     }

     public Histogram1D(String name, double start, double end, int nbins)
     {
     }
     */
    public void fill(Timestamp x) {
        //System.out.println("fill: x="+x);
        if (x == null)
            return;
        int nb = ((int)((x.getTime()-hStart)/binWidth))+1;
        if (nb < 0) nb = 0;
        if (nb > nBins) nb = nBins;
        hBody[nb] += 1.0;
        total += 1.0;
        //System.out.println("fill: hBody["+nb+"]="+hBody[nb]);
        entries += 1;
    }

    public void fill(Timestamp x, double w) {
        //System.out.println("fill: x="+x+" w="+w);
        if (x == null)
            return;
        int nb = ((int)((x.getTime()-hStart)/binWidth))+1;
        if (nb < 0) nb = 0;
        if (nb > nBins) nb = nBins;
        hBody[nb] += w;
        total += w;
        //System.out.println("fill: hBody["+nb+"]="+hBody[nb]);
        entries += 1;
    }

    public long getEntries() {
        return entries;
    }

    public double getTotal() {
        return total;
    }

    public int getNumberOfBins() {
        return nBins;
    }

    public double getY(int nbin) {
        int nb = nbin;
        if (nbin < 0) nb = 0;
        if (nbin > nBins) nb = nBins;
        return hBody[nb];
    }

    public Timestamp getX(int nbin) {
        int nb = nbin;
        if (nbin < 0) nb = 0;
        if (nbin > nBins) nb = nBins;
        return new Timestamp(hStart+binWidth*(nb-1));
    }

    public Timestamp getXmin() {
        return new Timestamp(hStart);
    }

    public Timestamp getXmax() {
        return new Timestamp(hEnd);
    }

    public double[] getBins() {
        return hBody;
    }

    public String getName() {
        return hName;
    }

    private String hName;
    private double[] hBody;

    private int  nBins;
    private long hStart;
    private long hEnd;
    private long binWidth;
    private long entries;
    private double total;
}

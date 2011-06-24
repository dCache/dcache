package gov.fnal.isd;
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

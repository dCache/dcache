/*
 * Adler32.java
 *
 * Created on April 27, 2005, 11:25 AM
 */

package org.dcache.srm.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 *
 * @author  timur
 */
public class Adler32 {
    
    /** Creates a new instance of Adler32 */
    public Adler32() {
    }
    
    public static final void main(String[] args) throws Exception{
        if(args==null || args.length != 1 || 
        args[0].equalsIgnoreCase("-h")    ||
        args[0].equalsIgnoreCase("-help") ||
        args[0].equalsIgnoreCase("--h")   ||
        args[0].equalsIgnoreCase("--help")  ) {
            System.err.println(" Usage: adler32 file");
            System.exit(1);
            return;
        }
        File f = new File(args[0]);
        if(!f.exists() ) {
            System.err.println("file "+args[0]+" does not exist");
            System.exit(2);

        }
        if(!f.canRead() ) {
            System.err.println("file "+args[0]+" can not be read ");
            System.exit(2);

        }

        FileChannel fc = new RandomAccessFile(f,"r").getChannel();
        String adler32 =GridftpClient.long32bitToHexString( GridftpClient.getAdler32(fc));
        System.out.println(adler32);
    }
}

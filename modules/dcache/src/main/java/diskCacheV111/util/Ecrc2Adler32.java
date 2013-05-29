/*
 * Ecrc2Adler32.java
 *
 * Created on December 17, 2003, 4:04 PM
 */

package diskCacheV111.util;

/**
 *
 * @author  timur
 */
public class Ecrc2Adler32 {

    private static int BASE = 65521 ;
    public static long convert(long ecrc,long filesize)
    {
        int size = (int)(filesize %BASE);
        int s1 = (int)(ecrc & 0xffffL);
        int s2 = (int)((ecrc>>16) & 0xffffL);
        s1 = (s1+1)%BASE;
        s2 = (size +s2)%BASE;
        return ((s2<<16)+s1)&0x0FFFFFFFFL;
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String usage  = "usage: Ecrc2Adler32 <file ecrc decimal> <filesize>";
        if(args.length != 2)
        {
            System.out.println(usage);
            System.exit(1);
        }
        long ecrc = Long.parseLong(args[0]);
        long filesize = Long.parseLong(args[1]);
        long adler32 = convert(ecrc,filesize);
        System.out.println("adler32 is "+adler32+" hex value "+Long.toHexString(adler32));

    }

}

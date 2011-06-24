/*
 * $Id: DumpSI.java,v 1.2 2006-04-12 09:26:53 tigran Exp $
 */
package diskCacheV111.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import diskCacheV111.vehicles.StorageInfo;

public class DumpSI {

    public static void main(String[] args) {

        int rc = 1;

        if( args.length != 1) {
            System.err.println("Usage: DumpSI <SI file>");
            System.exit(rc);
        }

        String siFile = args[0];

        try {
            FileInputStream fis = new FileInputStream(siFile);
            ObjectInputStream ois = new ObjectInputStream(fis);

            StorageInfo si = (StorageInfo) ois.readObject();
            ois.close();

            System.out.println();
            System.out.println(si.toString());

            rc = 0;
        } catch (FileNotFoundException fe) {
            System.err.println("File not found : " + fe.getMessage() );
        }catch(IOException ie) {
            System.err.println("IO exception : " + ie.getMessage());
        } catch (ClassNotFoundException ce) {
            System.err.println("Not a SI file : " + ce.getMessage() );
        }

        System.exit(rc);

    }

}

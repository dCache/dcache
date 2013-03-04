package gov.fnal.XMLList;

import java.io.File;

public class dataStore {
   private String startDir;
   private String outXMLFile;
   private String RepertoireCherche;
   private long lengthFile=0L;
   private long nbFile=0;
   private boolean recursive;

   public dataStore(File startPoint, File destXML,
      boolean recurse, boolean math1) {

      String dirAbsStr;

      dirAbsStr = startPoint.getAbsolutePath();
      startDir=dirAbsStr;
      outXMLFile=destXML.getAbsolutePath();
      recursive=recurse;
   }

   public String getDir() { return startDir; }


   public void setoutXMLFile(String a) { outXMLFile=a; }

   public String getoutXMLFile() { return outXMLFile; }

   public static String getFS() {
      return System.getProperty("file.separator");
   }

   public long getLengthFile() { return(lengthFile); }

   public void incLengthFile(long a) {
      lengthFile+=a;
      nbFile++;
   }

   public long getNbFile() { return nbFile; }

   public boolean doRecursion(){ return recursive; }

   public void setRecursive(boolean recurse) { recursive = recurse; }
}

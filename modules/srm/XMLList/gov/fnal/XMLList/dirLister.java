package gov.fnal.XMLList;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import gov.fnal.XMLList.XMLLister;

public class dirLister {

   private dataStore ds;
   private boolean recurse;
   private boolean doMath;
   private XMLLister lister;
   String sortOrder;
   String source;
   String dest;

   public dirLister(String inSource, String inDest, boolean inRecurse,
      String inSortOrder) {
      // source: Starting directory.
      // dest:  Name of XML output file.
      // recurse: recurse down into sub-directories
      // sortOrder:  of listings of files and directories.
      //    natural: use Java sorting (based on file name, from A to Z)
      //    FA: by file name from A to Z (same as previous)
      //    FZ: by file name from Z to A
      //    LA: by file length, ascending
      //    LZ: by file length, descending
      //    DA: by last modified date, ascending
      //    DZ: by last modified date, descending

      sortOrder = inSortOrder;
      recurse = inRecurse;
      source = inSource;
      dest = inDest;

   }


   public void doIt() {

      try {
         File sourceDir = new File(source);
         if(sourceDir.exists() && sourceDir.isDirectory() &&
            sourceDir.canRead()) {

            File destFile = new File(dest);

            if(!destFile.exists() ||
               (destFile.isFile() && destFile.canWrite())) {

               OutputStreamWriter destWriter =
                  new OutputStreamWriter(
                  new FileOutputStream(destFile), "UTF-8");

               ds = new dataStore(sourceDir, destFile, recurse, doMath);
               lister = new XMLLister(destWriter);
               System.out.println("Processing directory " + ds.getDir());
               System.out.println();
               doWrite(ds.getDir());
               lister.close();
            } else {
               System.out.println(
                  "A problem occurred with the destination file.");
            }

         } else {
            System.out.println(
               "A problem occurred with the source directory.");
         }

      } catch (IOException e) {
         System.out.println("Error!: "+e.toString());
         System.out.println();
      }
   }

   private void doWrite(String dirABS) throws IOException {

      lister.writeStartList();
      Directory dir =
         new Directory(ds, lister, dirABS, recurse, sortOrder);
      // dir.execute();
      lister.writeEndList();
   }

}

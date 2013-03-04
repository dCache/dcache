package gov.fnal.XMLList;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import gov.fnal.XMLList.XMLLister;

public class Directory {

   private boolean recurse;
   private dataStore ds;
   private XMLLister lister;
   File[] dirList;
   File[] fileList;
   int nbFile = 0;
   int nbDir = 0;
   String dirAbs;
   File dir;
   String way;

   public Directory(dataStore inds, XMLLister inLister,
      String inDirAbs, boolean inRecurse,
      String inWay) {

      recurse = inRecurse;
      ds = inds;
      lister = inLister;
      dirAbs = inDirAbs;
      dir = new File(inDirAbs);
      way = inWay;

      if ((dir.isDirectory()==true) && (dir.canRead()) ) {
         File contents[] = dir.listFiles();
         File tmpFileList[] = new File[contents.length];
         File tmpDirList[] = new File[contents.length];
         nbFile = 0;
         nbDir = 0;

         for (int i=0;i<contents.length;i++) {

            if (contents[i].isFile()) {
               tmpFileList[nbFile]=contents[i];
               nbFile++;
            } else {
               tmpDirList[nbDir]=contents[i];
               nbDir++;
            }
         }

         dirList = new File[nbDir];
         System.arraycopy(tmpDirList, 0, dirList, 0, nbDir);
         fileList = new File[nbFile];
         System.arraycopy(tmpFileList,0, fileList,0, nbFile);

         if (nbDir > 0) sortFile(dirList, nbDir, "natural");

         if (nbFile > 0) sortFile(fileList, nbFile, way);

         try {
            lister.writeDirList(dirList, nbDir, dirAbs);
            lister.writeStartDir(dir);
            writeFileList(fileList,nbFile);
            lister.writeEndDir(dir);

            if (recurse) recurseDirList(dirList, nbDir, way);
         }

         catch (IOException e){
            System.out.println(e);
         }

      }
   }


   public void execute() {


   }

   private void writeFileList(File[] fileList, int nbFile)
   throws IOException {

      for (int i=0; i<nbFile; i++)
         if ( (fileList[i].canRead()) &&
         (fileList[i].exists()) &&
         (fileList[i].isFile()) )
            writeFile(fileList[i]);
   }


   private void writeFile(File file) throws IOException {
      lister.writeStartFile(file);
      lister.writeEndFile(file);
      ds.incLengthFile(file.length());
   }


   private void recurseDirList(File[] dirList, int nbDir, String way)
   throws IOException {

      for (int i = 0; i < nbDir; i++)
         if ((dirList[i].canRead()) && (dirList[i].exists())) {
         Directory rec =
            new Directory(ds, lister, dirList[i].getAbsolutePath(),
            true, way);
         }
   }


   private void sortFile(File[] fileList, int nbFile,String way){
      if ((way.equalsIgnoreCase("natural")) || (way.equals(""))){

         Arrays.sort(fileList);
      } else {
         int i=0,j=0;
         boolean run=true;
         File t1;

         while (run) {
            run=false;
            i=j=0;
            for (i=0;i<fileList.length;i++) {
               j=i+1;
               if (j<fileList.length) {
                  //System.out.println(fileList[j].getAbsolutePath());
                  if (checkForChange(way,fileList[i],fileList[j])) {
                     run=true;
                     t1=fileList[i];
                     fileList[i]=fileList[j];
                     fileList[j]=t1;
                  }
               }
            }
         }
      }
   }



   private boolean checkForChange(String way2, File f1, File f2) {
      String way = way2.toUpperCase();

      if (way.startsWith("D")) {
         if (way.endsWith("Z")) {
            if (f2.lastModified()>f1.lastModified()) {
               return true;
            }
         } else {
            if (f2.lastModified()<f1.lastModified()) {
               return true;
            }
         }
      } else {
         if (way.startsWith("L")) {
            if (way.endsWith("Z")) {
               if (f2.length()>f1.length()) {
                  return true;
               }
            } else {
               if (f2.length()<f1.length()) {
                  return true;
               }
            }
         } else {
            String s1=f1.getAbsolutePath();
            String s2=f2.getAbsolutePath();
            int r=s1.compareTo(s2);

            if (way.endsWith("Z")) {
               if (r<0) {
                  return true;
               }
            } else {
               if (r>0) {
                  return true;
               }
            }
         }
      }
      return false;
   }
}

package gov.fnal.XMLList;

public class start{

   public static void main(String[] arg){

      int i=0;

      // Now chunk through the passed-in parameters 2 at a time,
      // generating lists each time.
      while (i < arg.length) {

         if ((arg[i]!=null) || (arg[i]!=" ")) {
            doOneList(arg[i], arg[i+1]);
            i++;
         }
         i++;
      }

   }

   private static void doOneList(String startPlace, String outFile) {

      gov.fnal.XMLList.dirLister inst =
         new gov.fnal.XMLList.dirLister(startPlace, outFile,
                                        true, // recurse
                                        "FA");
      inst.doIt();
      // See documentation for dirLister.java for explanation of above.
   }
}

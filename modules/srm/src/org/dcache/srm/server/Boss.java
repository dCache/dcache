package org.dcache.srm.server;

/**
 *
 * @author leoheska
 */
public class Boss {
   
   /** Creates a new instance of Boss */
   public Boss() {
   }

   final static int limit = 100000;
   
   static public boolean lsOK(int seeks, int finds) 
   throws TooManyHitsException
   {
      if (finds > limit) throw new TooManyHitsException(limit);
      if (seeks + finds < limit) return true;
      else return false;
   }
   
}

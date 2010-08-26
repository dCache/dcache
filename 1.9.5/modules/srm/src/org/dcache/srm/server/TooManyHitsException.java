package org.dcache.srm.server;

/**
 *
 * @author leoheska
 */
public class TooManyHitsException extends Exception {
   
   /** Creates a new instance of TooManyHitsException */
   public TooManyHitsException(int limit) {
      super("Results limited to " + limit);
   }
}

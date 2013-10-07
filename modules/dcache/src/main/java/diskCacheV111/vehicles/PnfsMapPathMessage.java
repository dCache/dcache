//$Id: PnfsMapPathMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class PnfsMapPathMessage extends PnfsMessage {

   private String _globalPath;
   private boolean _resolve;

   private static final long serialVersionUID = 1592692201158065130L;

   public PnfsMapPathMessage( String globalPath ){
      _globalPath = globalPath ;
      setPnfsPath(globalPath);
      setReplyRequired(true);
   }
   public PnfsMapPathMessage( PnfsId pnfsId ){
      super( pnfsId ) ;
      setReplyRequired(true);
   }

   public void setShouldResolve(boolean resolve) {
       _resolve = resolve;
   }

   public boolean shouldResolve() {
       return _resolve;
   }
   public String getGlobalPath(){ return _globalPath ; }
   public void setGlobalPath( String globalPath ){
      _globalPath = globalPath ;
      setPnfsPath(globalPath);
   }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }
}

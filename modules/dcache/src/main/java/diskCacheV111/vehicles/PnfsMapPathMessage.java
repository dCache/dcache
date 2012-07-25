//$Id: PnfsMapPathMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.PnfsId ;

public class PnfsMapPathMessage extends PnfsMessage {

   private String _globalPath;

   private static final long serialVersionUID = 1592692201158065130L;

   public PnfsMapPathMessage( String globalPath ){
      _globalPath = globalPath ;
      setReplyRequired(true);
   }
   public PnfsMapPathMessage( PnfsId pnfsId ){
      super( pnfsId ) ;
      setReplyRequired(true);
   }
   public String getGlobalPath(){ return _globalPath ; }
   public void setGlobalPath( String globalPath ){
      _globalPath = globalPath ;
   }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }
}

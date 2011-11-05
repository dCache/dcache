package dmg.cells.applets.login ;

public interface DomainEventListener {

   public void connectionOpened( DomainConnection connection ) ;
   public void connectionClosed( DomainConnection connection ) ;
   public void connectionOutOfBand( DomainConnection connection ,
                                    Object subject                ) ;
}

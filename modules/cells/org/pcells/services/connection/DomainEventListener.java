package org.pcells.services.connection ;

public interface DomainEventListener {

   public void connectionOpened( DomainConnection connection ) ;
   public void connectionClosed( DomainConnection connection ) ;
   public void connectionOutOfBand( DomainConnection connection ,
                                    Object subject                ) ;
}

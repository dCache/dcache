package org.pcells.services.connection ;

import java.io.IOException;
import java.io.Serializable;

public interface DomainConnection {

    public String getAuthenticatedUser();

    public int sendObject( Serializable obj ,
                           DomainConnectionListener listener ,
                           int id
                         ) throws IOException ;
    public int sendObject( String destination ,
                           Serializable obj ,
                           DomainConnectionListener listener ,
                           int id
                         ) throws IOException ;

    public void addDomainEventListener( DomainEventListener listener ) ;
    public void removeDomainEventListener( DomainEventListener listener ) ;

}

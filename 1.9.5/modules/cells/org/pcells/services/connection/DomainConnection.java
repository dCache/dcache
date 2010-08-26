package org.pcells.services.connection ;

import  java.io.* ;

public interface DomainConnection {

    public String getAuthenticatedUser(); 

    public int sendObject( Object obj , 
                           DomainConnectionListener listener ,
                           int id 
                         ) throws IOException ;
    public int sendObject( String destination ,
                           Object obj , 
                           DomainConnectionListener listener ,
                           int id 
                         ) throws IOException ;

    public void addDomainEventListener( DomainEventListener listener ) ;
    public void removeDomainEventListener( DomainEventListener listener ) ;

} 

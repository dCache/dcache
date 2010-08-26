package dmg.cells.applets.spy ;

public interface DomainConnectionListener {

    public void connectionActivated( DomainConnectionEvent event ) ;
    public void connectionDeactivated( DomainConnectionEvent event ) ;
    public void connectionFailed( DomainConnectionEvent event ) ;
} 

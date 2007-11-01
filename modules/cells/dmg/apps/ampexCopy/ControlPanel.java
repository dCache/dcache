package dmg.apps.ampexCopy ;

import java.awt.* ;
import java.awt.event. * ;
import java.io.* ;

public class ControlPanel 
       extends Panel
       implements ActionListener {
    private ActionListener _listener = null ;
    private File _dir = null ;
    public void addActionListener( ActionListener listener ){
      _listener = listener ;
    }
    private Button _backButton = null ;
    private Label  _dirLabel   = null ;
    public ControlPanel( File dir ){
        setLayout( new BorderLayout() ) ;
        _dir        = dir ;
        _backButton = new Button( "Back" ) ;
        String _path = null ;
        try{  
           _path = _dir.getCanonicalPath() ;
        }catch(Exception ee ){
           _path = "Unknown" ;
        }
        _dirLabel   = new Label( _path ) ;
        _backButton.addActionListener( this ) ;
        add( _backButton , "North" ) ;
        add( _dirLabel   , "South" ) ;
    }
    public void actionPerformed( ActionEvent event ){
       if( _listener != null )
          _listener.actionPerformed( event ) ;
    }
    
    
}

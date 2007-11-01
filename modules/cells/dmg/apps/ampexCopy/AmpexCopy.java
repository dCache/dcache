package dmg.apps.ampexCopy ;

import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;

public class      AmpexCopy 
       extends    Frame 
       implements WindowListener,
                  Runnable ,
                  ActionListener  { 

  private Font   _bigFont = 
            new Font( "SansSerif" , Font.BOLD , 18 )  ; 
  private Font   _bigFont2 = 
            new Font( "SansSerif" , Font.BOLD , 34 )  ;
  private Toolkit _toolkit     = Toolkit.getDefaultToolkit() ;
  private boolean _debug       = false ;
  private boolean _wasProgress = false ;
  private Display _display     = null ;
  private String [] _args      = null ;
  private File      _dir       = null ;
  private boolean   _okButton  = false ;
  
  private class PageDescriptor {
  
      private Vector _strings = null ;
      private Vector _colors  = null ;
      private Vector _progres = null ;
      private String _status  = "" ;
      private String _ok      = null ;
      private boolean _useImage = false ;
      
      public PageDescriptor(){
         _useImage = true ;
      }
      public boolean shouldUseImage(){ return _useImage ; }
      public PageDescriptor( String status ){
         _status = status ;
      }
      public PageDescriptor( String status , String ok ){
         _status = status ;
         _ok     = ok ;
      }
      public void addLine( String str ){
         addLine( str , Color.black ) ;
      }
      public void addLine( String str , Color color ){
         if( _strings == null ){
             _strings = new Vector() ;
             _colors  = new Vector() ;
         }
         _strings.addElement( str ) ;
         _colors.addElement( color );
      }
      public Color getColor( int i ){ 
          return (Color)_colors.elementAt(i) ;
      }
      public int getLineCount(){ 
          return  _strings == null ? 0 : _strings.size() ; 
     }
      public String getLine(int i ){
         return  (String)_strings.elementAt(i) ;
      }
      public String getOk(){ return _ok ; }
      public String getStatus(){ return _status ; }
      public int getProgress(int i){ 
          return ((Integer)_progres.elementAt(i)).intValue() ;
      }
      public int getProgressCount(){ 
          return _progres == null ? 0 : _progres.size() ;
      }
      public void addProgress( String name , int i ){
         if( _progres == null ){
             _progres = new Vector() ;
             _strings = new Vector() ;
         }
         _strings.addElement( name ) ;
         _progres.addElement( new Integer(i) ) ;
      }
  }
  private class CodeEvent extends ActionEvent {
     private int [] _code = null ;
     private int    _count = 0 ;
     private CodeEvent( Object source , int [] code , int count ){
        super( source , 0 , "CodeEvent" ) ;
        _code = code ;
        _count = count ;
     
     }
     public int getCount(){ return _count  ; }
     public int [] getCode(){ return _code ; } 
  
  }
  private class Display extends Canvas {
     private String         _title = "Ampex Copy Manager" ;
     private Rectangle      _buttonRect   = null ;
     private boolean        _mousePressed = false ;
     private PageDescriptor _defaultPage  = new PageDescriptor() ;
     private PageDescriptor _currentPage  = _defaultPage ;
     private Object         _pageLock     = new Object() ;
     private int             bound        =  10 ;
     private Rectangle   [] _click        = new Rectangle[10] ;
     private int         [] _codeStore    = new int[_click.length] ;
     private int            _codeCount    = 0 ;
     private Color          _frameColor   = Color.white ;
     private ActionListener _listener     = null ;
     public Display( String title ){
         addMouseListener( new MouseAction() ) ;
         _title = title ;
         _defaultPage.addLine( "Please Wait" , Color.red ) ;
     }
     public void addActionListener( ActionListener listener ){
        _listener = listener ;
     } 
     
     public class MouseAction extends MouseAdapter {
     
         public void mouseClicked( MouseEvent me ){
            Point mousy = me.getPoint() ;
            for( int i = 0 ; i < _click.length ; i++ ){
               if( _click[i].contains( mousy ) ){
                  _toolkit.beep() ;
                  if( i == 0 ){
                    if( _listener != null )
                       _listener.actionPerformed(
                               new CodeEvent( this , _codeStore , _codeCount )
                                                 ) ;
                    _codeCount = 0 ;
                                                
                  }else{
                    _codeCount %= _codeStore.length ;
                    _codeStore[_codeCount++] = i ;
                  }
                  return ;
               }
            }
            if( ! _okButton )return ;
            Rectangle br = _buttonRect ;
            if( br == null )return ;
            if( ! br.contains( mousy ) )return ;
            synchronized( _pageLock ){
               _currentPage = _defaultPage ;
            }
            repaint() ;
            new File( _dir , "OkButton" ).delete() ;
         }
         public void mousePressed( MouseEvent me ){
            if( ! _okButton )return ;
            Rectangle br = _buttonRect ;
            if( br == null )return ;
            if( ! br.contains( me.getPoint() ) )return ;
            _mousePressed = true ;
            repaint() ;
         }
         public void mouseReleased( MouseEvent me ){
            if( _mousePressed ){
               _mousePressed = false ;
               repaint() ;
            }
         }
     }
     public void setPage( PageDescriptor page ){
         synchronized( _pageLock ){
            if( page == null ){
               if( _currentPage == _defaultPage ){
                  return ;
               }else{
                  _currentPage = _defaultPage ;
               }
            
            }else{
               _currentPage = page ;
            }
         }
         repaint() ;
     }
     public void refresh(){ repaint() ; }
     private Image _image     = null ;
     private File  _imageFile = null ;
     private int   _codeWidth = 40 ;
     private boolean loadImage(){
        if( _image != null )return true ;
        if( _imageFile == null )_imageFile = new File( _dir , "wait.jpg" ) ;
        
        if( ! _imageFile.exists() )return false ;
       
        FileInputStream is = null ;
        try{
           is           = new FileInputStream( _imageFile ) ;
           int     size = (int)_imageFile.length() ;
           byte [] data = new byte[size] ;
           is.read( data ) ;
           _image       = _toolkit.createImage(data) ;
        }catch(Exception ie){
           return false ;
        }finally{
           try{ is.close() ; }catch(Exception ee){}
        }
        return true ;
     
     }
     public void update( Graphics g ){
        PageDescriptor page = null ;
        synchronized( _pageLock ){
           page         = _currentPage ;
        }
        boolean isProgress = page.getProgressCount() > 0 ;
        boolean updateOnly = isProgress && _wasProgress ;
        _wasProgress = isProgress ;
        
        paint( g ,   page , updateOnly ) ;
     }
     public void paint( Graphics g ){
        PageDescriptor page = null ;
        synchronized( _pageLock ){
           page         = _currentPage ;
        }
        paint( g , page , false ) ;
     }
     public void paint( Graphics g , 
                        PageDescriptor page ,
                        boolean update ){
         Dimension d = getSize() ;
         //
         // the background
         //
         if( ! update ){
            g.setColor(Color.white) ;
            g.fillRect(0,0,d.width-1,d.height-1);
         
         //
            if( page.shouldUseImage() && loadImage() ){
               int height = _image.getHeight(null) ;
               int width  = _image.getWidth(null);
               if( ( height < 0 ) || ( width < 0 ) )return  ;
               System.out.println( "Image ; "+width+";"+height);
               int x = ( d.width - width ) / 2 ;
               int y = ( d.height - height ) / 2 ;
               g.drawImage( _image , x , y , null ) ;
               return ;
            }
         }
         //
         Rectangle center = new Rectangle() ;
         //
         // the header line
         //
         g.setFont( _bigFont2 ) ;
         FontMetrics fm = g.getFontMetrics() ;
         int width = fm.stringWidth(_title) ;
         int height = fm.getAscent()+fm.getDescent() ;
         if( ! update ){
            g.setColor(Color.red) ;
            g.fillRect( bound , bound , d.width - 2*bound , height ) ;
            g.setColor(Color.blue);
            g.drawString( _title , (d.width-2*bound-width)/2  , 
                                    bound + fm.getAscent()) ;
         }
         center.x      = bound + _codeWidth ;
         center.y      = 2 * bound + height ;
         center.width  = d.width - 2 * bound - 2* _codeWidth ;
         center.height = d.height - 2 * bound - height ;
         if( page == null )return ;
         //
         // the status line
         //
         String statusLine = page.getStatus() ;
         statusLine = statusLine == null ? "" : statusLine ;
         if( statusLine != null ){
            g.setFont( _bigFont ) ;
            fm     = g.getFontMetrics() ;
            width  = fm.stringWidth(statusLine) ;
            height = fm.getAscent()+fm.getDescent() ;
            if( ! update ){
               g.setColor(Color.black) ;
               g.fillRect( bound , d.height-bound-height ,
                           d.width - 2*bound , height ) ;
               g.setColor(Color.yellow) ;
               g.drawString( statusLine , bound+10 , 
                             d.height-bound-fm.getDescent() ) ;
            }
            center.height -= ( 2 * bound + height ) ;
         
            //
            //
            //
            if( _frameColor != null ){
               g.setColor(_frameColor);
               g.drawRect(center.x,center.y,center.width,center.height);
            }
         }else{
            System.err.println("--> Status line == null " ) ;
         }
         //
         // the code entries
         //
         if( ! update ){
            int count = _click.length ;
            int codeHeight = ( center.height - ( count-1 )* bound ) / count ;
            int y = center.y ;
            int diff = codeHeight + bound ;
            for( int i = 0 ; i < count ; i++ ){
               g.setColor( new Color( (float).4 , 
                                      (float)0.6 ,
                                      (float) 1. / (float)count * (float)i ) ) ;
               _click[i] = new Rectangle(
                              bound + bound , 
                              y , 
                              _codeWidth - 2 * bound , 
                              codeHeight ) ;
               g.fillRect( _click[i].x , 
                           _click[i].y , 
                           _click[i].width ,
                           _click[i].height  ) ;
               g.setColor( new Color( (float).9 , 
                                      (float)0.2 ,
                                      (float) 1. / (float)count * (float)i ) ) ;
               g.fillRect( center.x + center.width + bound , 
                           y , _codeWidth - 2 * bound , codeHeight ) ;
               y += diff ;
            }
         }
         if( page.getProgressCount() > 0 ){
             drawProgress( g , center , page , update  ) ;
             return ;
         }
         Rectangle in = new Rectangle( center ) ;
         //
         // the ok button
         //
         String okLine = page.getOk() ;
         if( okLine != null ){
            g.setFont( _bigFont2 ) ;
            fm = g.getFontMetrics() ;
            int fb  =  4 ;
            width  = fm.stringWidth(okLine) + fb ;
            height = fm.getAscent()+fm.getDescent() + fb ;


            Rectangle rec = new Rectangle(
                  center.x + ( center.width - width ) / 2 ,
                  center.y + center.height - bound - height  ,
                  width ,
                  height ) ;

            _buttonRect = rec ;
            g.setColor(_mousePressed?Color.blue:Color.green) ;
            g.fillRect(rec.x,rec.y,rec.width,rec.height) ;      
            g.setColor(Color.black) ;
            g.drawRect(rec.x-1,rec.y-1,rec.width+1,rec.height+1) ;      
            g.setColor(_mousePressed?Color.green:Color.blue) ;
            g.drawRect(rec.x-2,rec.y-2,rec.width+3,rec.height+3) ; 
            g.drawString(okLine,rec.x+fb/2,rec.y+fm.getAscent()+fb/2); 
            
            in.height -= ( 2 * bound + rec.height ) ;
            
            if( ! _okButton ){
               g.setColor(Color.red) ;
               g.drawLine(rec.x-8,rec.y-8,
                          rec.x+rec.width+8,rec.y+rec.height+8) ;      
               g.drawLine(rec.x-8,rec.y+rec.height+8,
                          rec.x+rec.width+8,rec.y-8 ) ;
                    
            
            }
         } 
         if( _frameColor != null ){   
            g.setColor(_frameColor);
            g.drawRect(in.x,in.y,in.width,in.height);
         }
         g.setFont( _bigFont2 ) ;
         fm = g.getFontMetrics() ;
         int fb   =  4 ;
         height   = fm.getAscent()+fm.getDescent()  ;
         int n    = page.getLineCount() ;
         int diff = in.height / ( n + 1 ) ;
         int y    = in.y ;
         for( int i = 0 ; i < n ; i++ ){
             String msg = page.getLine(i) ;
             g.setColor(page.getColor(i));
             y += diff ;
             width = fm.stringWidth(msg) ;
             g.drawString(msg,
                          in.x+(in.width-width)/2,
                          y+fm.getAscent()/2) ; 
         }
     }
     private void drawProgress( Graphics g ,
                                Rectangle center ,
                                PageDescriptor page ,
                                boolean update       ){
          
         g.setFont( _bigFont2 ) ;
         FontMetrics fm = g.getFontMetrics() ;
         int n        = page.getProgressCount() ;
         int diff     = center.height / ( n + 1 ) ;
         int y        = center.y ;
         int thick    = 30 ;
         int maxWidth = 0 ;
         int height   = fm.getAscent()+fm.getDescent()  ;
         for( int i =  0 ; i < n ; i++ ){
            int width = fm.stringWidth(page.getLine(i)) ;
            maxWidth = maxWidth > width ? maxWidth : width ;
         }
         for( int i =  0 ; i < n ; i++ ){
             y += diff ;
             g.setColor( Color.black ) ;
             g.drawRect( center.x + 2*bound + maxWidth , 
                         y - thick/2 ,
                         center.width - 3 * bound - maxWidth ,
                         thick  ) ;
             int total = center.width - 3 * bound -1 - maxWidth ;
             int p = page.getProgress(i) ;
             int frac = (int)((((double)p)/100.)*(double)total) ;
             g.setColor( Color.red ) ;
             g.fillRect( center.x + 2*bound + 1 + maxWidth , 
                         y - thick/2 + 1 ,
                         frac ,
                         thick -1  ) ;
         
//             if( ! update ){
                g.setColor( Color.blue ) ;
                g.drawString( page.getLine(i) ,
                              center.x + bound ,
                              y + thick/2 - fm.getDescent() ) ; 
//             }
         }                               
                                
     }
  
  }
  private ControlPanel _control    = null ;
  private CardLayout   _cards      = new CardLayout() ;
  private Panel        _cardsPanel = null ;
  public AmpexCopy( String [] args ) throws Exception {
      super( "AmpexCopy Manager" ) ;
      _args = args ;
      _dir  = new File( _args[0] ) ;
      if( ! _dir.isDirectory() ){
         System.err.println( "Not a directory : "+_dir ) ;
         System.exit(4) ;
      }
      setLayout( _cards ) ;
      addWindowListener( this ) ;
      
      setLocation( 60 , 60) ;
  
      _display = new Display("Ampex Copy Manager") ;
      _control = new ControlPanel( _dir ) ;
      add( _display , "display" ) ;
      add( _control , "control" ) ;
      _cards.show( this , "display" ) ;
      _display.addActionListener( this ) ;
      _control.addActionListener( this ) ;
      setSize( 550 , 400 ) ;
      pack() ;
      setSize( 550 , 400 ) ;
      setVisible( true ) ;
      
      new Thread( this ).start() ;
  
  }
  public void actionPerformed( ActionEvent ae ){
     if( ae instanceof CodeEvent ){
        CodeEvent ce = (CodeEvent)ae ;
//        System.out.println( "Code event length : "+ce.getCount() ) ;
        int count = ce.getCount() ;
        int [] code = ce.getCode() ;
        if( ( count == 4 ) &&
            ( code[0] == 1 ) &&
            ( code[1] == 2 ) &&
            ( code[2] == 9 ) &&
            ( code[3] == 1 )   ){
              _cards.show( this  , "control" ) ;
        }else if( ( count == 4 ) &&
            ( code[0] == 1 ) &&
            ( code[1] == 2 ) &&
            ( code[2] == 3 ) &&
            ( code[3] == 4 )   ){
            System.exit(0);
        }
     }else{
        _cards.show( this , "display" ) ;
     }
  }
  public void run(){
     try{
        long lastUpdate = 0L , lm = 0L ;
        boolean mExists   = false ;
        boolean doRefresh = true ;
        while(true){
            if( doRefresh )_display.refresh() ;
            doRefresh = false ;
            Thread.sleep(2000) ;
            //
            // refresh whenever the OkButton state changes.
            //
            File q = new File( _dir , "OkButton" ) ;
            boolean okButton = q.exists() ;
            if( _okButton ^ okButton ){
               _okButton = okButton ;
               doRefresh = true ;
            }
            //
            // if 'messages' exists we ignore the progess
            //
            File m = new File( _dir , "messages" ) ;
            if( m.exists() ){
               lm  = m.lastModified() ;
               if( ( lm > lastUpdate ) || ( ! mExists ) ){
                   runMessageUpdate(m) ;
                   _toolkit.beep() ;
                   lastUpdate = lm ;
               } 
               mExists = true ;
               continue ;
            }else if( mExists ){
               lastUpdate = 0L ;
               mExists = false ;;
            }
            
            File p = new File( _dir , "progress" ) ;
            if( p.exists() ){
               lm  = p.lastModified() ;
               if( lm > lastUpdate ){
                   runProgressUpdate(p) ;
                   lastUpdate = lm ;
               }
               continue ;
            }
            _display.setPage( null ) ;
        }
     }catch(Exception e){
         System.err.println( "Problem in : "+e ) ;
         System.exit(4);
     }
  }
  private void runMessageUpdate(File m ){
     BufferedReader br = null ;
     try{
        br = new BufferedReader( new FileReader( m ) ) ;
        String line    = null ;
        String ok     = br.readLine()  ;
        if( ok == null )return ;
        String status = br.readLine() ;
        if( status == null )return ;
        PageDescriptor page = 
             new PageDescriptor( status , ok.equals("*")?null:ok ) ;
        StringTokenizer st = null ;
        while( ( status = br.readLine() ) != null ){
           st = new StringTokenizer(status,"=");
           int n = st.countTokens() ;
           if( n == 0 )continue ;
           if( n == 1 ){
               page.addLine( st.nextToken() ) ;
           }else{
               StringTokenizer stt = new StringTokenizer(st.nextToken(),",");
               int l = stt.countTokens() ;
               if( l < 3 )continue ;
               try{
                  Color color = new Color( 
                               Integer.parseInt(stt.nextToken()) ,
                               Integer.parseInt(stt.nextToken()) ,
                               Integer.parseInt(stt.nextToken()) 
                                    ) ;
                  page.addLine( st.nextToken() , color ) ;
               }catch(Exception ie ){
                   continue ;
               }
           }
        }
        _display.setPage( page ) ;
     }catch(Exception ee ){
        if(_debug)ee.printStackTrace() ;
     }finally{
       try{ br.close() ; }catch(Exception ee ){}
     }
  }
  private void runProgressUpdate(File m ){
     BufferedReader  br   = null ;
     PageDescriptor  page = null ;
     StringTokenizer st   = null ;
     String line = null , status = null ;
     try{
        br      = new BufferedReader( new FileReader( m ) ) ;
        line    = null ;
        status  = br.readLine() ;
        page    =  new PageDescriptor( status ) ;
        while( ( status = br.readLine() ) != null ){
           try{
              st = new StringTokenizer(status,"=") ;             
              page.addProgress( st.nextToken() ,
                                Integer.parseInt(st.nextToken()) ) ;
           }catch(Exception ie ){
           }
        }
     }catch(Exception ee ){
        ee.printStackTrace() ;
     }finally{
       try{ br.close() ; }catch(Exception ee ){}
     }
     String [] fileList = _dir.list(
            new FilenameFilter(){
               public boolean accept( File dir , String name ){
                  return name.startsWith("progress.") ;
               }
            }
            ) ;
            
     for( int i = 0 ; i < fileList.length ; i++ ){
        File x = new File( _dir , fileList[i] ) ;
       
        try{
           br      = new BufferedReader( new FileReader( x ) ) ;
           line    = null ;
           while( ( status = br.readLine() ) != null ){
              try{
                 st = new StringTokenizer(status,"=") ;
                 page.addProgress( st.nextToken() ,
                                   Integer.parseInt(st.nextToken()) ) ;
              }catch(Exception ie ){
              }
           }
        }catch(Exception ee ){
           if(_debug)ee.printStackTrace() ;
        }finally{
          try{ br.close() ; }catch(Exception ee ){}
        }
     
     }
     _display.setPage( page ) ;
  }
  //
  // window interface
  //
  public void windowOpened( WindowEvent event ){}
  public void windowClosed( WindowEvent event ){
      System.exit(0);
  }
  public void windowClosing( WindowEvent event ){
      System.exit(0);
  }
  public void windowActivated( WindowEvent event ){}
  public void windowDeactivated( WindowEvent event ){}
  public void windowIconified( WindowEvent event ){}
  public void windowDeiconified( WindowEvent event ){}
  public static void main( String [] args ){
      if( args.length < 1 ){
        System.err.println( "Usage : ... <controlDir>" ) ;
        System.exit(4);
      }
      
      try{
            
         new AmpexCopy( args ) ;
      
      }catch( Exception e ){
         e.printStackTrace() ;
         System.exit(4);
      }
      
  }

}

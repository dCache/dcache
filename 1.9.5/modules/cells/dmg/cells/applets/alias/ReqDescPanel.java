package dmg.cells.applets.alias ;

import java.lang.reflect.* ;
import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;


public class ReqDescPanel 
       extends Panel
       implements ActionListener {

   private int _b = 11 ;
   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 18 )  ; 
   private Font   _bigFont2 = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 
   public Insets getInsets(){ return new Insets( _b , _b ,_b , _b ) ; }
   public void paint( Graphics g ){
      Dimension   d    = getSize() ;
      Color base = getBackground() ;
      g.setColor( Color.white ) ;
      g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
   }
   /*
   public void paint( Graphics g ){
      Dimension   d    = getSize() ;
      Color base = getBackground() ;
      for( int i = 0 ; i < _b ; i++ ){
         g.setColor( base ) ;
         g.drawRect( i , i , d.width-2*i-1 , d.height-2*i-1 ) ;
         base = base.darker() ;
      }
   }
   */
   private Object _theObject = null ;
   private Class  _theClass  = null ;
   private Panel  _container = null ;
   public ReqDescPanel( Class c , Object o ){
      setLayout( new BorderLayout() ) ;
      
      _theObject = o ;
      _theClass  = c ;
      
      Label topLabel = new Label( c.getName() , Label.CENTER ) ;
      topLabel.setFont( _bigFont ) ;
      add( topLabel  , "North" ) ;
      
      
      if( ! c.getSuperclass().equals( java.lang.Object.class ) )
      add( new ReqDescPanel( c.getSuperclass() , o ) , "Center" ) ;
      
      Method [] m = c.getMethods() ;
      Hashtable setHash = new Hashtable() ;
      Hashtable getHash = new Hashtable() ;
      
      for( int i = 0 ; i < m.length ; i++ ){
         if( ! m[i].getDeclaringClass().equals(c) )continue ;
         String name = m[i].getName() ;
         if( name.startsWith("get")  ){
             if( m[i].getParameterTypes().length > 0 )continue ;
             getHash.put( m[i].getName().substring(3) , m[i] ) ;
         }else if( name.startsWith("set") ){
             if( m[i].getParameterTypes().length == 0 )continue ;
             setHash.put( m[i].getName().substring(3) , m[i] ) ;
         }else{
            continue ;
         }
      }
      String      key  = null  ;
      Method      x    = null ;
      Hashtable   hash = new Hashtable() ;
      Method []   a    = null ;
      Enumeration e    = getHash.keys() ;
      
      while( e.hasMoreElements() ){
         key  = (String)e.nextElement() ;
         a    = new Method[2] ;  // must be exactly here.
         a[0] = x = (Method)setHash.get( key ) ;
         if(  ( x == null ) ||
              ( x.getParameterTypes().length > 1 ) )continue ;
         a[1] = (Method)getHash.get( key ) ;
         hash.put( key , a ) ;
      }
      e = hash.keys() ;
      while( e.hasMoreElements() ){
         key = (String)e.nextElement() ;
         getHash.remove( key ) ;
         setHash.remove( key ) ;
      }
      ReqItemPanel tmp = null ;
      _container = new Panel( new GridLayout(0,1) ) ;
      e          = hash.keys() ;
      while( e.hasMoreElements() ){
         key = (String)e.nextElement() ;
         a   = (Method [])hash.get( key ) ;
         _container.add( tmp = new ReqItemPanel( key , a[0] , a[1] ) ) ;
         tmp.addActionListener(this) ;
      }
      e = setHash.keys() ;
      while( e.hasMoreElements() ){
         key = (String)e.nextElement() ;
         x   = (Method )setHash.get( key ) ;
         _container.add( tmp = new ReqItemPanel( key , x , null ) ) ;
        tmp.addActionListener(this) ;
      }
      e = getHash.keys() ;
      while( e.hasMoreElements() ){
         key = (String)e.nextElement() ;
         x = (Method )getHash.get( key ) ;
         _container.add( tmp = new ReqItemPanel( key , null , x ) ) ;
        tmp.addActionListener(this) ;
      }
      add( _container , "South" ) ;
   }
   public void actionPerformed( ActionEvent event ){
      Object source = event.getSource() ;
//      System.out.println( "Event in ReqDescPanel ; "+event ) ;
      Component [] c = _container.getComponents() ;
      for( int i= 0 ;i < c.length ; i++ ){
//         System.out.println( "c[i]="+c[i].getClass().getName() ) ;
         if( c[i] instanceof ReqItemPanel ){
            ReqItemPanel rip = (ReqItemPanel)c[i] ;
            if( rip.isSetter() )rip.refresh() ;  
         }   
      }
      for( int i= 0 ;i < c.length ; i++ ){
//         System.out.println( "c[i]="+c[i].getClass().getName() ) ;
         if( c[i] instanceof ReqItemPanel ){
            ReqItemPanel rip = (ReqItemPanel)c[i] ;
            if( ! rip.isSetter() )rip.refresh() ;  
         }   
      }
   }
   private class ReqItemPanel 
                 extends Panel 
                 implements ActionListener,
                            KeyListener     {
       private TextField [] _text  = null ;
       private Label        _label = null ;
       private boolean      _both  = false ;
       private ActionListener _actionListener = null ;
       private Method   _setter = null , _getter = null ;
       private String   _name   = null ;
       ReqItemPanel( String name , Method setter , Method getter ){
           super( new GridLayout(1,0) ) ;
//           System.out.println( "Setting "+name ) ;
           _name   = name ;
           _setter = setter ;
           _getter = getter ;
           Label l ;
           add( l = new Label( name , Label.LEFT ) ) ;
//           l.setFont( _bigFont ) ;
           Object result ;
           if( _setter == null ){
             // this is the getter
             //
//             System.out.println( "Getter : "+_getter.getName() ) ;
             add( _label = new Label( "" , Label.CENTER ) );
             try{
                result = _getter.invoke( _theObject , new Object[0] ) ;
             }catch( Exception e ){
                result = "???" ;
             }
             if( result == null )result = "" ;
             add( _label = new Label( result.toString() , Label.CENTER ) ) ;
             refresh() ;
           }else if( _getter == null ){
             //
             //setter
             //
             Class [] pc = _setter.getParameterTypes() ;
             _text = new TextField[pc.length] ;
//             System.out.println( "Setter : "+_setter.getName()+" n="+_text.length ) ;
             for( int i= 0 ;i < _text.length ; i++ ){
                add( _text[i] = new TextField("") ) ;
                _text[i].addActionListener( this ) ;
                _text[i].addKeyListener( this ) ;
             }
//            refresh();
           }else{
//             System.out.println( "Getter/Setter : "+_getter.getName()+"/"+_setter.getName() ) ;
             _both = true ;
             _text = new TextField[1] ;
             add( _text[0] = new TextField("") ) ;
             _text[0].addActionListener( this ) ;
             _text[0].addKeyListener( this ) ;
             refresh() ;
           }
       }
       public void refresh(){
           Object result ;
           String str = null ;
           if( _setter != null ){
             int l = 0 ;
             for( int i = 0 ;i < _text.length ; i++ )
               if( ! _text[i].getText().equals("") )l++ ;
             
             if( l == _text.length ){
                Class [] pc = _setter.getParameterTypes() ;
                Object [] args = new Object[pc.length] ;
                for( int i = 0 ; i < pc.length ; i++ ){
                    str = _text[i].getText() ;
                    args[i] = stringToObject( str.equals("^")?"":str , pc[i] ) ;
                    _text[i].setText("");
                }         
                try{
                   _setter.invoke( _theObject , args ) ;
                }catch( Exception e ){
                   System.out.println( "Setter failed : "+e )  ;
                }
             }
           
           }
           if( _getter != null ){
             //
             // this is the getter
             //
             try{
                result = _getter.invoke( _theObject , new Object[0] ) ;
             }catch( Exception e ){
                result = "???" ;
             }
             if( result == null )result = "" ;
             if( _label != null )_label.setText( result.toString() ) ;
             if( _text  != null )_text[0].setText( result.toString() ) ;
           }
           /*
           if( _text != null ){
              for( int i = 0 ; i < _text.length ; i++ ){
                    _text[i].setBackground( Color.white ) ;
              }
           }
           */
       
       }
       public void addActionListener( ActionListener al ){
          _actionListener  = al ;
//          System.out.println( "Action listener added for : "+_name ) ;
       }
       public void actionPerformed( ActionEvent event ){
           Object source = event.getSource() ;
           if( source instanceof TextField ){
              ((TextField)source).setBackground(Color.white) ;
           }
          if( _actionListener != null ){
             _actionListener.actionPerformed( 
                   new ActionEvent( this , 0 , "repaint" ) ) ;
          }
         
       }
       public void keyPressed( KeyEvent event ){
//           Object source = event.getSource() ;
       }            
       public void keyReleased( KeyEvent event ){
//           Object source = event.getSource() ;
       }            
       public void keyTyped( KeyEvent event ){
           Object source = event.getSource() ;
           if( source instanceof TextField ){
              ((TextField)source).setBackground(Color.red) ;
           }
       }            
       public boolean isSetter(){ return _setter != null ; }
       private Object  stringToObject( String str , Class c ){
          Object result = null ;
          if( c.equals( int.class ) ){
             try{
                result =  Integer.valueOf( str ) ;
             }catch( Exception e ){
                result =  Integer.valueOf(0) ;
             }
          }else if( c.equals( long.class ) ){
             try{
                result =  Long.valueOf( str ) ;
             }catch( Exception e ){
                result =  Long.valueOf(0) ;
             }          
          }else if( c.equals( float.class ) ){
             try{
                result =  new Float( str ) ;
             }catch( Exception e ){
                result =  new Float(0.0) ;
             }          
          }else if( c.equals( java.lang.Object.class ) ){
              result =  str ;
          }else if( c.equals( java.lang.String.class ) ){
              result =   str ;
          }
//          System.out.println( "String >"+str+"< -> ("+
//                              result.getClass().getName()+") "+result ) ;
          return result ;
       }
   }

} 

package gov.fnal.XMLList;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.Map;

public class XMLLister extends PrintWriter {

   protected final String ianaEncoding;


   public XMLLister(OutputStreamWriter out) {
      super(out);
      ianaEncoding = toIANAName(out.getEncoding());

   }


   private static final String toIANAName(String encoding) {
      if(encoding.equals("UTF8"))
         return "UTF-8";
      else if(encoding.equals("ISO8859_1"))
         return "ISO-8859-1";
      else
         return encoding;
   }


   public void writeStartList() {
      printXmlPI();
      printDoctype("srmls:dirlisting", "http://www-isd.fnal.gov/srm/XML/dirlisting.dtd");
      println("<srmls:dirlisting xmlns:srmls=\"http://www-isd.fnal.gov/srm/XML/dirlisting\">");
   }
   public void writeEndList() {
      println("</srmls:dirlisting>");
   }
   public void writeDirList(File[] dirList, int nbDir, String dirABS) {}
   public void writeStartDir(File dir) {
      print("   <srmls:directory");
      printAttribute("name", dir.getName());
      try {
         printAttribute("url", dir.toURL().toString());
      } catch(MalformedURLException e) {
      } finally {
         println(">");
      }
   }
   public void writeEndDir(File dir) {
      println("   </srmls:directory>");
   }
   public void writeStartFile(File file) {
      print("      <srmls:file");
      printAttribute("name", file.getName());
      printAttribute("size", Long.toString(file.length()));
      // printAttribute("type", type);
      try {
         printAttribute("url", file.toURL().toString());
      } catch(MalformedURLException e) {
      } finally {
         println(">");
      }
   }
   public void writeEndFile(File file) {
      println("      </srmls:file>");
   }
   public void writeAttributes(Map attributes) {
      Iterator iter = attributes.keySet().iterator();
      while(iter.hasNext()) {
         String name = iter.next().toString();
         String value = attributes.get(name).toString();
         print("         <srmls:attribute");
         printAttribute("name", name);
         printAttribute("value", value);
         println("/>");
      }
   }

   protected final void printXmlPI() {
      println("<?xml version=\"1.0\" encoding=\""+ianaEncoding+"\"?>");
   }
   protected final void printDoctype(String name, String publicId, String systemId) {
      print("<!DOCTYPE ");
      print(name);
      print(" PUBLIC \"");
      print(publicId);
      print("\"\n \"");
      print(systemId);
      println("\">");
   }
   protected final void printDoctype(String name, String systemId) {
      print("<!DOCTYPE ");
      print(name);
      print(" SYSTEM \"");
      print(systemId);
      println("\">");
   }
   protected final void printAttribute(String name, String value) {
      print(" ");
      print(name);
      print("=\"");
      print(xmlEncode(value));
      print("\"");
   }
   protected static final String xmlEncode(String str) {
      final int len = str.length();
      StringBuffer buf = new StringBuffer(str);
      for(int i=len-1; i>=0; i--) {
         switch(str.charAt(i)) {
            case '&' : buf.insert(i+1, "amp;"); break;
            case '<' : buf.setCharAt(i, '&'); buf.insert(i+1, "lt;"); break;
            case '>' : buf.setCharAt(i, '&'); buf.insert(i+1, "gt;"); break;
            case '\"' : buf.setCharAt(i, '&'); buf.insert(i+1, "quot;"); break;
         }
      }
      return buf.toString();
   }
}


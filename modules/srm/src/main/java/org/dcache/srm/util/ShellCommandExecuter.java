/*
 * ShellCommandExecuter.java
 *
 * Created on January 28, 2003, 1:40 PM
 */

package org.dcache.srm.util;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Writer;
/**
 *
 * @author  timur
 */

public class ShellCommandExecuter implements Runnable
{
    public static int execute(String command)
    {
        return execute(command,new java.io.PrintWriter(System.out),new java.io.PrintWriter(System.err));
    }
    
    public static int execute(String command,Writer out, Writer error)
    {

         //System.out.println("execute("+command+")");
         Process proc;
         InputStream StdErr;
         InputStream StdOut;

        try
        {
            proc = Runtime.getRuntime().exec(command);
            StdErr = proc.getErrorStream();
            StdOut  = proc.getInputStream();
        }
        catch(IOException ioe)
        {
            ioe.printStackTrace();
            return 1;
        }

        java.io.BufferedReader OutReader = 
            new java.io.BufferedReader(new java.io.InputStreamReader(StdOut));
        new ShellCommandExecuter(OutReader,out);
        java.io.BufferedReader ErrReader = 
            new java.io.BufferedReader(new java.io.InputStreamReader(StdErr));
        new ShellCommandExecuter(ErrReader,error);
        int exit_value=1;
        try
        {
            exit_value =  proc.waitFor();
        }
        catch(InterruptedException ie)
        {
        }
        //System.out.println(" exit value is "+ exit_value);
        return exit_value;
    }
    public static int execute(String [] command,Writer out, Writer error)
    {

         //System.out.println("execute("+command+")");
         Process proc;
         InputStream StdErr;
         InputStream StdOut;

        try
        {
            proc = Runtime.getRuntime().exec(command);
            StdErr = proc.getErrorStream();
            StdOut  = proc.getInputStream();
        }
        catch(IOException ioe)
        {
            ioe.printStackTrace();
            return 1;
        }

        java.io.BufferedReader OutReader = 
            new java.io.BufferedReader(new java.io.InputStreamReader(StdOut));
        new ShellCommandExecuter(OutReader,out);
        java.io.BufferedReader ErrReader = 
            new java.io.BufferedReader(new java.io.InputStreamReader(StdErr));
        new ShellCommandExecuter(ErrReader,error);
        int exit_value=1;
        try
        {
            exit_value =  proc.waitFor();
        }
        catch(InterruptedException ie)
        {
        }
        //System.out.println(" exit value is "+ exit_value);
        return exit_value;
    }

    public static String[] executeAndReturnOutput(String command)
    {

         //System.out.println("executeAndReturnOutput("+command+")");
         Process proc;
         InputStream StdErr;
         InputStream StdOut;

        try
        {
            proc = Runtime.getRuntime().exec(command);
            StdErr = proc.getErrorStream();
            StdOut  = proc.getInputStream();
        }
        catch(IOException ioe)
        {
            ioe.printStackTrace();
            return null;
        }
               
        java.io.StringWriter string_writer = new java.io.StringWriter();
        java.io.BufferedReader OutReader = 
            new java.io.BufferedReader(new java.io.InputStreamReader(StdOut));
        new ShellCommandExecuter(OutReader,string_writer);
        java.io.BufferedReader ErrReader = 
            new java.io.BufferedReader(new java.io.InputStreamReader(StdErr));
        new ShellCommandExecuter(ErrReader,new java.io.PrintWriter(System.err));
        try
        {
            proc.waitFor();
        }
        catch(InterruptedException ie)
        {
        }
        //System.out.println(" exit value is "+ exit_value);
        java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(string_writer.getBuffer().toString());
        int len = tokenizer.countTokens();
        String result[] = new String[len];
        for(int i =0; i<len;++i)
        {
            result[i] = tokenizer.nextToken();
        }
        return result;
    }

    java.io.BufferedReader reader;
    java.io.BufferedReader ErrReader;
    boolean error;
    private java.io.Writer out;
    
    private  ShellCommandExecuter(java.io.BufferedReader reader,java.io.Writer out)
    {
        
        this.reader = reader;
        this.out = out;
        new Thread(this).start();
    }


  @Override
  public void run()
  {
      try
      {
            String line;
            while((line = reader.readLine()) != null)
            {
                out.write(line);
                out.write('\n');
                out.flush();
            }
      }
      catch(IOException e)
      {
            e.printStackTrace();
      } 
  }
}

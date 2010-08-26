package infoDynamicSE;

import java.util.*;
import java.util.regex.*;

/**
 * Command line Options Handler.<br>
 * This is an utility class that allow to manage easily the arguments
 * passed from the command line to a <code>main(String args[])</code>
 * method and handle them as options for the whole application. 
 * Passing the <code>args[]</code> String to this class, it will parse 
 * every String in the array to find a pattern like:<br>
 * <code>-&lt;attribute&gt;=&lt;value&gt;</code><br>
 * The option pairs found will be put in an Hashtable that allow to 
 * retrive an option value providing its attribute name.
 **/
public class Opts {
	
	/** Where the option pairs are stored **/
	static private Hashtable _tab;
	
	/**
	 * This method parse the String array passed to find
	 * the pattern <code>-&lt;attribute&gt;=&lt;value&gt;</code>, and
	 * when find it, puts the pairs in the Hashtable.
	 * @param argv
	 */
	static public void parse(String[] argv){
		_tab = new Hashtable();
		Pattern p = Pattern.compile("\\-(.*[^=])=(.*)");
		for(int i=0;i<argv.length; i++){
			//System.out.println(argv[i]);
			Matcher m = p.matcher(argv[i]);
			while(m.find()) _tab.put(m.group(1),m.group(2));
		}
	}
	
	/**
	 * This method allow to retrive a value of an option providing
	 * its attribute name.
	 * @param arg
	 * @return value
	 */
	static public String get(String arg){
		return (String)_tab.get(arg);
	}
	
	/**
	 * This is a test method that if renamed in <code>main</code> allow to
	 * test directly this class, launching this class and providing some 
	 * option at the command line. This method will return the content of the
	 * table, so the attribute/value pairs parsed. 
	 * @param argv
	 */
	static public void test(String[] argv){
		
		parse(argv);
		
		Iterator it = _tab.keySet().iterator();
		
		System.out.println("\n<option>,<value>\n-----------------");
		while(it.hasNext()){
			String option = (String)it.next();
			String value = (String)_tab.get(option);
			System.out.println(option+","+value);
		}
		System.out.print('\n');
	}
}

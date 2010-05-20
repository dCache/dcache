#!/usr/bin/env ruby


require 'optparse'
require 'optparse/time'
require 'ostruct'
require 'pp'
require 'open-uri'
require 'rexml/document'




def processXML ( inFile, outFile, vos, options )
  doc = REXML::Document.new( File.new(inFile))
  if options.has_key?("SITE-UNIQUE-ID")
    doc.elements['//[@id="SITE-UNIQUE-ID"]'].text = options["SITE-UNIQUE-ID"]
  end
  if options.has_key?("SE-UNIQUE-ID")
    doc.elements['//[@id="SE-UNIQUE-ID"]'].text = options["SE-UNIQUE-ID"]
  end
  if options.has_key?("SE-NAME")
    doc.elements['//[@id="SE-NAME"]'].text = options["SE-NAME"]
  end
  if options.has_key?("DCACHE-STATUS")
    doc.elements['//[@id="DCACHE-STATUS"]'].text = options["DCACHE-STATUS"]
  end
  if options.has_key?("DCACHE-ARCHITECTURE")
    doc.elements['//[@id="DCACHE-ARCHITECTURE"]'].text = options["DCACHE-ARCHITECTURE"]
  end
  e = doc.elements['//[@name="SRM-supported-VOs"]']
  e.delete_if { true }
  vos.each do |vo|
    e.add_element("item").text = vo
  end
  e = doc.elements['//[@name="GlueSA-VOs"]']
  if e != nil
    e.delete_if { true }
    vos.each do |vo|
      e.add_element("item").text = vo
    end
  end
  if options.has_key?("NAME-SPACE-PREFIX")
    e = doc.elements['//[@name="VO-name-to-path"]']
    if e != nil
      e.delete_if { true }
      vos.each do |vo|      
        e.add_element("sub", { "match" => vo, "replace-with" => options["NAME-SPACE-PREFIX"] + "/" + vo })
      end
      e.add_element("default", { "value" => "/UNDEFINEDPATH" })
    end
  end
  if options.has_key?("UNIT2VO")
    e = doc.elements['//[@name="unit-to-VO"]']
    if e != nil
      options["UNIT2VO"].each do |mapping|
        maparray = mapping.split('^')
        if maparray.length() == 2
          newe = e.add_element("sub", { "match" => maparray[0], "replace-with" => maparray[1] })
          e.text = "\n"
        else
          print "Error processing mapping '" + mapping + "'/n'"
        end
      end
      e.add_element("sub", { "match" => "*@*", "replace-with" => "" })
    end
  end
  if options.has_key?("VO2PATH")
    e = doc.elements['//[@name="VO-to-path"]']
    if e != nil
      options["VO2PATH"].each do |mapping|
        maparray = mapping.split('^')
        if maparray.length() == 2
          newe = e.add_element("sub", { "match" => maparray[0], "replace-with" => maparray[1] })
          e.text = "\n"
        else
          print "Error processing mapping '" + mapping + "'/n'"
        end
      end
    end
  end
  if options.has_key?("UNIT2PATH")
    e = doc.elements['//[@name="unit-to-path"]']
    if e != nil
      options["UNIT2PATH"].each do |mapping|
        maparray = mapping.split('^')
        if maparray.length() == 2
          newe = e.add_element("sub", { "match" => maparray[0], "replace-with" => maparray[1] })
          e.text = "\n"
        else
          print "Error processing mapping '" + mapping + "'/n'"
        end
      end
    end
  end
  File.open(outFile, "w") { |f| f.puts doc }
end


vos = []
options = {}

fileInput = ""
fileOutput = ""

ARGV.options do |o|
  script_name = File.basename($0)
  
  o.set_summary_indent('  ')
  o.banner =    "Usage: #{script_name} [options]"
  o.define_head "Infor provider XSL configuration tool."
  o.separator   ""
  o.separator   "Mandatory arguments to long options are mandatory for " +
                "short options too."


  o.on("-i", "--input [EXTENSION]",
              "Input XML file path",
              "  (XML file to read)")   { |fileInput| }
  o.on("-o", "--output [EXTENSION]",
              "Output XML file path",
              "  (XML file to write)")   { |fileOutput| }

  o.on("-s", "--site-unique-id [EXTENSION]",
              "GlueSiteUniqueID",
              "A unique reference for your site. This must match the GlueSiteUniqueID defined in other services.")   { |options["SITE-UNIQUE-ID"]| }

  o.on("-S", "--se-unique-id [EXTENSION]",
              "GlueSEUniqueID",
              "Your dCache's Unique ID.  Currently, this *must* be the FQDN of your SRM end-point.")   { |options["SE-UNIQUE-ID"]| }

  o.on("-n", "--se-name [EXTENSION]",
              "GlueSEName",
              "A human understandable name for your SE (it may contain spaces).  You may leave this empty and a GlueSEName will not be published.")   { |options["SE-NAME"]| }

  o.on("-u", "--dcache-status [EXTENSION]",
              "GlueSEStatus",
              "Current status of dCache.  This should be one of the following values: Production, Queuing, Closed, Draining.")   { |options["DCACHE-STATUS"]| }

  o.on("-a", "--dcache-architecture [EXTENSION]",
              "GlueSEArchitecture",
              "The architecture of the underlying storage dCache is using.")   { |options["DCACHE-ARCHITECTURE"]| }

  o.on("-p", "--name-space-prefix [EXTENSION]",
              "Name space prefix.",
              "This describes which paths to publish for each VO.")   { |options["NAME-SPACE-PREFIX"]| }

  o.on("-U", "--unit2vo unit1^vo1,unit2^vo2",
              Array,
              "Array of Unit-VO mappings")   { |options["UNIT2VO"]| }
  o.on("-W", "--unit2path unit1^path1,unit2^path2",
              Array,
              "Array of Unit-Path mappings")   { |options["UNIT2PATH"]| }
  o.on("-Y", "--vo2path vo1^path1,vo2^path2",
              Array,
              "Array of VO-Path mappings")   { |options["VO2PATH"]| }

  # List of arguments.
  o.on("-V","--vos x,y,z", Array, "A 'list' of vo's") { |vos| }
              

  
  o.separator ""

  o.on_tail("-h", "--help", "Show this help message.") { puts o; exit }
  
  o.parse!
end
#pp options
#pp object


if options.has_key?("DCACHE-STATUS")
  if !["Production","Queuing","Closed","Draining"].include?(options["DCACHE-STATUS"])
    print "GlueSEStatus can only be set to Production, Queuing, Closed, Draining\n\n"
    print "Production       The SE processes old and new requests\n"
    print "                  according to its policies.\n\n"
    print "Queuing          The SE can accept new requests, but they\n"
    print "                  will be kept on hold\n\n"
    print "Closed           The SE does not accept new requests and\n"
    print "                  does not process old requests\n\n"
    print "Draining         The SE does not accept new request but\n"
    print "                  still processes old requests.\n"
    exit 1
  end
end

if options.has_key?("DCACHE-ARCHITECTURE")
  if !["disk","multidisk","tape"].include?(options["DCACHE-ARCHITECTURE"])
    print "GlueSEArchitecture: the architecture of the underlying\n"
    print " storage dCache is using.  This should be one of the\n"
    print "disk             non-robust, single-disk storage\n\n"
    print "multidisk        disk-based storage that is robust against\n"
    print "                  single disk failures.\n\n"
    print "tape             dCache has access to an HSM system.\n"
    exit 1
  end
end

if fileInput == ""
  print "Input file needs to be stated on the command line.\n"
  exit 1
end

if fileOutput == ""
  print "Output file needs to be stated on the command line.\n"
  exit 1
end


processXML(fileInput,fileOutput,vos,options)

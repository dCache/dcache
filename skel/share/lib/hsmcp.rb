#!/usr/bin/env ruby

require 'fileutils'
require 'uri'

def usage 
  STDERR.puts "Usage : put <pnfsId> <filePath> -hsmBase=<path> -hsmInstace=<name> -si=<storageInfo> [-key[=value] ...]" 
  STDERR.puts "        get <pnfsId> <filePath> -hsmBase=<path> -uri=<uri> [-key[=value] ...]" 
  STDERR.puts "        remove -uri=<uri> -hsmBase=<path> [-key[=value] ...]" 
  exit 4
end

#
# Parse options
#
options = Hash.new
args = Array.new

ARGV.each do |arg|
  case arg
  when /-(\w[\w:]+)=(\S+)/
    options[$1] = $2
  else
    args << arg
  end
end

# 
# Parse arguments
#
if args.length < 1
   usage
end
command=args[0]
case command
when "get","put","next"
  if args.length != 3
    usage
  end
  pnfsid=args[1]
  file=args[2]
end

# 
# Check options
#
if options.has_key? "errorNumber"
  error = options["errorNumber"]
  case error
  when 41
    STDERR.puts "No space left on device"
  when 42
    STDERR.puts "OSM disk read IOError"
  when 43
    STDERR.puts "OSM disk write IOError"
  else
    STDERR.puts "ErrorNumber-#{error}"
  end
  exit error.to_i
end

if !options.has_key? "hsmBase"
  STDERR.puts "Need 'hsmBase' ... " 
  exit 2
end

if options.has_key? "waitTime"
  waitTime = Integer(options["waitTime"])
else
  waitTime = 1
end

base=options["hsmBase"]
if !FileTest.directory? base
  STDERR.puts "Not a directory : #{base}"
  exit 5
end

# 
# Interpret command
#
case command 
when "get"
  hsmFile="#{base}/#{pnfsid}"

  if ! FileTest.file? hsmFile
    STDERR.puts "pnfsid not found: #{pnfsid}"
    exit 4
  end

  if options.has_key? "hsmError" 
    exit options["hsmError"]
  end
   
  sleep waitTime

  FileUtils.cp(hsmFile, file)

when "put"
  hsmFile="#{base}/#{pnfsid}"

  if FileTest.file? hsmFile
    STDERR.puts "pnfsid already exists, flushing anyway: #{pnfsid}"
  end

  if !options.has_key? "hsmInstance"
    STDERR.puts "Need -hsmInstance for put"
    exit 1
  end
  instance=options["hsmInstance"]

  if !options.has_key? "si"
    STDERR.puts "Need -si for put"
    exit 1
  end
  si = Hash.new
  options["si"].split(";").each do |s|
    if s =~ /(\w+)=(.*)/ 
      si[$1]=$2
    end
  end

  sClass = si["sClass"]
  if sClass =~ /([^:]+):(.+)/
    store = $1
    group = $2
  else
    STDERR.puts "Invalid storage class: #{sClass}"
    exit 6
  end

  FileUtils.cp(file, hsmFile)

  sleep waitTime
  
  puts URI.escape("hsm://#{instance}/?store=#{store}&group=#{group}&bfid=#{pnfsid}")

when "remove"
  if !options.has_key? "uri"
    STDERR.puts "Need -uri for remove"
    exit 2
  end

  uri = URI.parse(options["uri"])
  query = Hash.new
  uri.query.split("&").each do |s|
    if s =~ /(\w+)=(.*)/ 
      query[$1]=$2
    end
  end

  if !query.has_key? "bfid"
    STDERR.puts "bfid missing from URI: " + uri
    exit 6
  end

  sleep waitTime

  pnfsid = query["bfid"]
  hsmFile="#{base}/#{pnfsid}"

  # Notice that this is not an error condition
  if !FileTest.file? hsmFile
    STDERR.puts hsmFile + " not found"
    exit 0
  end

  begin
    FileUtils.rm(hsmFile)
  rescue 
    STDERR.puts "Failed to delete " + hsmFile
    exot 4
  end
  
when "next"
  

else
  STDERR.puts "Illegal command $command"
  exit 4
end


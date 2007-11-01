#!/usr/bin/env ruby

require 'fileutils'

def usage 
  STDERR.puts "Usage : put|get <pnfsId> <filePath> [-si=<storageInfo>] [-key[=value] ...]" 
  exit 4
end

# echo "$* `date`" >>/tmp/hsm.log

options = Hash.new
args = Array.new

ARGV.each do |arg|
  case arg
  when /-(\w+)=(\S+)/
    options[$1] = $2
  else
    args << arg
  end
end

if args.length != 3
   usage
end

command=args[0]
pnfsid=args[1]
file=args[2]

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
  exit error
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

hsmFile="#{base}/#{pnfsid}"

case command 
when "get"
  if ! FileTest.file? hsmFile
    STDERR.puts "pnfsid not found: #{pnfsid}"
    exit 4
  end

  if options.has_key? "hsmError" 
    exit options["hsmError"]
  end
   
  sleep waitTime

  FileUtils.cp(hsmFile, file)

#   if [ $? -ne 0 ] ; then
#      echo "Failed : cp $BASE/$pnfsid $filename" 1>&2
#      exit 5
#   fi

when "put"
  if FileTest.file? hsmFile
    STDERR.puts "pnfsid already exists : #{pnfsid}"
    exit 4
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
  if sClass =~ /(\w+):(\w+)/
    store = $1
    group = $2
  else
    STDERR.puts "Invalid storage class: #{sClass}"
    exit 6
  end

  FileUtils.cp(file, hsmFile)

  # if [ $? -ne 0 ] ; then
  #    echo "Failed : cp $filename $BASE/$pnfsid" 1>&2
  #    exit 5
  # fi

  sleep waitTime
  
  puts "hsm://#{instance}?store=#{store}&group=#{group}&bfid=#{pnfsid}"

when "next"
  

else
  STDERR.puts "Illegal command $command"
  exit 4
end


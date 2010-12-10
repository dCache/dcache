from optparse import OptionParser, OptionValueError
import logging
import parse_site_info_def
import parse_site_info_env
import validate_site_info
import parse_modules
import parse_targets
import os
import sys



def log_level_debug(logger):
    logger.setLevel(logging.DEBUG)

def log_level_info(logger):
    logger.setLevel(logging.INFO)

def log_level_warning(logger):
    logger.setLevel(logging.WARNING)

def log_level_error(logger):
    logger.setLevel(logging.ERROR)

def log_level_critical(logger):
    logger.setLevel(logging.CRITICAL)

debug_options = {'DEBUG' : log_level_debug,
    'INFO'     : log_level_info,
    'WARNING'  : log_level_warning,
    'ERROR'    : log_level_error,
    'CRITICAL' : log_level_critical,
    'NONE'     : log_level_critical,
    'ABORT'    : log_level_critical,
}
def basedir():
    output = os.getcwd()
    if "DCACHE_CONFIGURE_HOME" in os.environ:
        output = os.environ['DCACHE_CONFIGURE_HOME']
    return output


def dir_targets():
    output = "dCacheConfigure/targets"
    if "DCACHE_CONFIGURE_TARGETS" in os.environ:
        output = os.environ['DCACHE_CONFIGURE_TARGETS']
    return output

def dir_modules():
    output = "dCacheConfigure/modules"
    if "DCACHE_CONFIGURE_MODULES" in os.environ:
        output = os.environ['DCACHE_CONFIGURE_MODULES']
    return output

def dir_pmodules():
    output = "dCacheConfigure/pmods"
    if "DCACHE_CONFIGURE_PMODS" in os.environ:
        output = os.environ['DCACHE_CONFIGURE_PMODS']
    return output



def configurelist(option, opt_str, value, parser):
    targets = parse_targets.target_parser(dir_targets(),dir_modules(),dir_pmodules(),"/")
    configuretagets = targets.list_targets()
    if not value in configuretagets:
        print "The following configuration targets exist:"
        for i in configuretagets:
            print i
        raise OptionValueError("Configuration target must be set.")
    if not hasattr(parser.values, "cfglist"):
        parser.values.cfglist = []
    parser.values.cfglist.append(value)


def invalid_target_list(targetparser,targetlist):
    output = []
    listofvalidtargets = targetparser.list_targets()
    for item in targetlist:
        if item not in listofvalidtargets:
            output.append(item)
    return output

def display_target_list(targetparser):
    print "The Following configuration targets exist:"
    for i in targetparser.list_targets():
        print i
    return


def display_target_information(targetparser,information_list):
    invalid_list = invalid_target_list(targetparser,information_list)
    if len(invalid_list) != 0:
        for item in invalid_list:
            print("Error: '%s' is an invalid Target." % (item))
        display_target_list(targetparser)
        return False
    for target_name in information_list:
        print "Target: %s" % (target_name)
        print "Description: %s" % (targetparser.info_target(target_name))
        moduleslist = targetparser.info_modules(target_name)
        module_count = 0
        for module in moduleslist:
            module_count = module_count +1
            print "Module %s: %s" % (module_count,module['name'])
            if 'description' in module.keys():
                print " Description: %s" % (module['description'])
            if 'notes' in module.keys():
                print " Notes: %s" % (module['notes'])
            print " Language: %s" % (module['language'])

def main():
    logging.basicConfig()
    logger = logging.getLogger("dCacheConfigure")
    log_level_default = "WARNING"
    if "DCACHE_CONFIGURE_HOME" in os.environ:
        sys.path.append(os.environ['DCACHE_CONFIGURE_HOME'])
    parser = OptionParser()
    parser.add_option("-s", "--site-info", dest="siteinfo",
        help="The file conforming to the site-info.def to lay out your dCache.", metavar="FILE")
    parser.add_option("-c", "--configure",
        action="callback", callback=configurelist,
        type="string", nargs=1,
        help="The area of dCache you wish to configure.")
    parser.add_option("-l", "--list-targets",
        dest="listtargets", action="store_true", default=False,
        help="The area of dCache you wish to configure.")
    parser.add_option("-m", "--modules-dir", dest="modulesdir",default=str(dir_modules()),
        help="The base directory of the dCacheConfiure modules.", metavar="DIR")
    parser.add_option("-t", "--target-dir", dest="targetsdir",default=str(dir_targets()),
        help="The base directory of the dCacheConfiure target.", metavar="DIR")
    parser.add_option("-u", "--utile-dir", dest="utils",
        help="The base directory of the dCacheConfiure utils.", metavar="DIR")
    parser.add_option("-d", "--dcache-home", dest="dcachehome",
        help="The location of dCache HOME.", metavar="DIR")
    parser.add_option("-e", "--enviroment", dest="env",default=False,action="store_true",
        help="Parse siteinfo from the enviroment.")
    parser.add_option("-p", "--pmods", dest="pmods",default=str(dir_pmodules()),
        help="The directory to search for python modules.")
    parser.add_option("-i", "--info-target", dest="target_info",default=None,
        help="The directory to search for python modules.")
    parser.add_option("-D", "--debug-level", dest="debug",default=None,
        help="The debug level for the application overrids site-info.def value.")
    parser.add_option("-H", "--hostname", dest="hostname",default=None,
        help="""Sets the hostname: In linux this should be set to `hostname -f`. With Solaris `hostname -f` will set the hostname to '-f'""")

    (options, args) = parser.parse_args()

    targets = parse_targets.target_parser( options.targetsdir,options.modulesdir,options.pmods,"/")
    if options.listtargets == True:
        for mod in targets.list_targets():
            print mod
    config = {}
    if options.env:
        logger.info("reading enviroment")
        newcfg = parse_site_info_env.parse_env()
        for key in newcfg.keys():
            config[key] = newcfg[key]
    if options.siteinfo:
        logger.info("reading site-info.def")
        newcfg = parse_site_info_def.parse_site_info(options.siteinfo)
        for key in newcfg.keys():
            config[key] = newcfg[key]
    if config.has_key('YAIM_LOGGING_LEVEL'):
        log_level_default = config['YAIM_LOGGING_LEVEL']
    if options.debug:
        log_level_default = options.debug
        config['YAIM_LOGGING_LEVEL'] = options.debug
    try:
        debug_options[log_level_default](logger)
    except :
        print "The debug level '%s' is invalid" % (log_level_default)
        print "The following debug levels exist:"
        for key in debug_options.keys():
            print key
        sys.exit(1)
    if options.target_info != None:
        if not display_target_information(targets,[options.target_info]):
            sys.exit(1)
    if hasattr(parser.values, "cfglist"):
        if options.hostname == None:
            logger.error("Hostname must be set. Use -h prameter to show help")
            sys.exit(1)
        for target in parser.values.cfglist:
            targets.execute_target(target,config,options.hostname)

if __name__ == "__main__":
    main()

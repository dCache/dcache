import os.path
import commands
import logging

""" execute_module_bash and execute_module_python use similar code
This could be refactored using some abstraction patterns, or
alternatively the bash code coudl be ported to python and removed.
"""

logger = logging.getLogger("dCacheConfigure.execute_module_bash")

class execute_module_bash(Exception):
       def __init__(self, value):
           self.parameter = value
       def __str__(self):
           return repr(self.parameter)



def whereis(program):
    # this shoudl be clieaned up.
    for path in os.environ.get('PATH', '').split(':'):
        if os.path.exists(os.path.join(path, program)) and \
           not os.path.isdir(os.path.join(path, program)):
            return os.path.join(path, program)
    return None

def validate_fields(module):
    required_fields = ["path_files","path_directories","run"]
    #TODO: Shoudl use sets here.
    for req in required_fields:
        if not req in module.keys():
            logger.error("Error: No '%s' found in '%s' module definition." % (req,module['name']))
            return False
    optional_fields = ["check","requirements"]
    #TODO: Shoudl use sets here.
    for req in optional_fields:
        if not req in module.keys():
            logger.info("No '%s' found in '%s' module definition." % (req,module['name']))
    return True


def get_files(basedir,module):
    # Be more dry and merge code for python and bash
    resolvedfiles = {}
    directories = module['path_directories']
    files = module['path_files']
    for filename in files:
        foundfile = None
        for thisdir in directories:
            if foundfile != None:
                break
            currentdir = os.path.join(basedir,thisdir)
            if not os.path.isdir(currentdir):
                continue
            testfilepath = os.path.join(currentdir,filename)
            if os.path.isfile(testfilepath):
                foundfile = testfilepath
        resolvedfiles[filename] = foundfile
    return resolvedfiles


def cfg(module,resolvedfiles,config):
    # Retcode for bash shodul be 0 on success
    # other values indicate failure.
    outrc = True
    cmd = ""
    for item in module['path_files']:
	fp = resolvedfiles[item]
	cmd += ". %s\n" % (fp)
    cmd += str(module['run'])
    oldenv = os.environ
    for item in config.keys():
	#for every key thats not excluded set the env
	if not item in ['SHELLOPTS']:
	    os.environ[item] = config[item]
    status,output = commands.getstatusoutput(cmd)
    os.environ = oldenv
    if status == 0:
        logger.info(output)
        outrc = True
    else:
        logger.critical("Command failed %s" % cmd)
        logger.warning(output)
        outrc = False
    return outrc

def execute_module(module,config,hostname,basedir):
    rc = validate_fields(module)
    if not rc:
        return rc
    resolvedfiles = get_files(basedir,module)
    for item in resolvedfiles.keys():
        if None == resolvedfiles[item]:
            msg = "Could not find file '%s'." % (item)
            logger.critical(msg)
            raise execute_module_bash(msg)
    rc = cfg(module,resolvedfiles,config)
    return rc

import os.path
import commands
import sys
import logging

logger = logging.getLogger("dCacheConfigure.execute_module_python")

class execute_module_python(Exception):
       def __init__(self, value):
           self.parameter = value
       def __str__(self):
           return repr(self.parameter)


def validate_fields(module):
    # TODO move to using sets rather than simulating them.
    required_fields = ["path_files","path_directories","run"]
    for req in required_fields:
        if not req in module.keys():
            logger.error("Error: No '%s' found in '%s' module definition." % (req,module['name']))
            return False
    optional_fields = ["check","requirements"]
    for req in optional_fields:
        if not req in module.keys():
            logger.warning("Warning: No '%s' found in '%s' module definition." % (req,module['name']))
    return True


def get_files(basedir,module):
    """ TBD duplication of code in bash executor;
    consider refactoring to remove duplication.
    """
    resolvedfiles = {}
    directorys = module['path_directories']
    files = module['path_files']
    for filename in files:
        foundfile = None
        for thisdir in directorys:
            if foundfile != None:
                continue
            currentdir = os.path.join(basedir,thisdir)
            if not os.path.isdir(currentdir):
                continue
            testfilepath = os.path.join(currentdir,filename)
            if os.path.isfile(testfilepath):
                foundfile = testfilepath
        resolvedfiles[filename] = foundfile
    return resolvedfiles


def cfg(module,resolvedfiles,config,hostname):
    """ TBD: THis function modifies sys.path.
    Doing this is not exactly good practice and shoudl be looked into.
    """

    module_with_check = None
    module_with_run = None
    for item in module['path_files']:
        fp = resolvedfiles[item]
        filepath = resolvedfiles[item]
        if os.path.isfile(filepath):
            import_module = None
            config_path, config_file = os.path.split(filepath)
            if (config_path != '') and (not config_path in sys.path):
                sys.path.append(config_path)
            module_name, ext = os.path.splitext(config_file) # Handles no-extension files, etc.
            if ext != '.py':
                # Important, ignore .pyc/other files.
                continue
            import_module = __import__(module_name,globals(), locals(),0)
            if import_module == None:
                continue
            if module_with_check == None:
                if 'check' in module.keys():
                    module_with_check = import_module
            if module_with_run == None:
                if 'run' in module.keys():
                    module_with_run = import_module
    if 'check' in module.keys():
        if module_with_check == None:
            msg = "Module '%s' could not find the check method '%s'." % (module['name'],module['check'])
            raise execute_module_python(msg)
    if module_with_run == None:
        msg = "Module '%s' could not find the run method" % (module['name'])
        raise execute_module_python(msg)
    if not hasattr(module_with_run, module['run']):
        msg = "Error: Module definition '%s' specifies run method '%s' but method not found." % (module['name'],module['run'])
        raise execute_module_python(msg)
    runner = getattr(module_with_run,module['run'])
    return runner(config,hostname)



def execute_module(module,config,hostname,basedir):
    rc = validate_fields(module)
    if not rc:
        msg = "Error: finding fields in module definition '%s'."
        raise execute_module_python(msg)
    resolvedfiles = get_files(basedir,module)
    for item in resolvedfiles.keys():
        if None == resolvedfiles[item]:
            msg = "Error: could not find file '%s'." % (item)
            raise execute_module_python(msg)
    rc = cfg(module,resolvedfiles,config,hostname)
    return rc

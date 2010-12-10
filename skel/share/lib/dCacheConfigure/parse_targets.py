import os.path
import parse_modules
import sys
import ConfigParser, os
import logging
logger = logging.getLogger("dCacheConfigure.parse_targets")

class target_parser_exception(Exception):
       def __init__(self, value):
           self.parameter = value
       def __str__(self):
           return repr(self.parameter)


class target:
    def __init__(self,path):
        pass


class target_parser:
    def __init__(self, targetdir, moduledir,dir_mod_base_py,dir_mod_base_sh):

        self.target_list = []
        self.targetdir = targetdir
        self.moduledir = moduledir
        if not os.path.isdir(targetdir):
            msg = "The directory '%s' does not exist." % (targetdir)
            raise target_parser_exception(msg)
        if not os.path.isdir(str(moduledir)):
            msg = "The directory '%s' does not exist." % (moduledir)
            raise target_parser_exception(msg)
        self.modules = parse_modules.module_parser(moduledir,dir_mod_base_py,dir_mod_base_sh)

        self.scan_targets()
    def scan_targets(self):
        self.modules.scan_modules()
        target_list = []
        for dirent in os.listdir(str(self.targetdir)):

            filepath = os.path.join(self.targetdir,dirent)
            if os.path.isfile(filepath):
                module_name, ext = os.path.splitext(filepath) # Handles no-extension files, etc.
                if ext == '.cfg': # Important, ignore .pyc/other files.
                    import_target = ConfigParser.ConfigParser()
                    import_target.read(filepath)

                    for section in import_target.sections():
                        if not import_target.has_option(section,"modules"):
                            continue
                        if not import_target.has_option(section,"description"):
                            continue
                        modulestring =  import_target.get(section,"modules")
                        list_of_mods = modulestring.split(',')
                        cleaned_list = []
                        for item in list_of_mods:
                            striped_item = item.strip()
                            if self.modules.registered(striped_item):
                                cleaned_list.append(item.strip())
                            else:
                                msg = "The Target '%s' requires a module '%s' that is not available." % (section,striped_item)
                                raise target_parser_exception(msg)

                        mod = {
                            'name':str(section),
                            'description':str(import_target.get(section,"description")),
                            'modules':cleaned_list
                        }
                        target_list.append(mod)
        self.target_list = target_list

    def list_targets(self):
        output = []
        for target in self.target_list:
            output.append(target['name'])
        return output

    def execute_target(self,target_name,cfg,hostname):
        for target in self.target_list:
            if target_name == target['name']:
                for module_name in target['modules']:
                    logger.info("Executing module %s" % (module_name))
                    rc = self.modules.execute_module(module_name,cfg,hostname)
                    if rc != True:
                        msg = "The module '%s' returned a error code." % (module_name)
                        raise target_parser_exception(msg)
        return rc

    def info_target(self,target_name):
        output = None
        for target in self.target_list:
            if target_name == target['name']:
                output = target['description']
        return output


    def info_modules(self,target_name):
        output = []
        for target in self.target_list:
            if target_name == target['name']:
                for module in target['modules']:
                    output.append(self.modules.info(module))
        return output

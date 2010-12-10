import os.path
import execute_module_bash
import execute_module_python
import sys
import ConfigParser, os
import logging
logger = logging.getLogger("dCacheConfigure.parse_modules")

class module_parser:
    def __init__(self, moduledir,dir_mod_base_py,dir_mod_base_sh):
        self.dir_mod_base_py = dir_mod_base_py
        self.dir_mod_base_sh = dir_mod_base_sh
        self.module_list = ()
        self.moduledir = moduledir
        if not os.path.isdir(moduledir):
            raise got_to_code_this
        self.scan_modules()

    def scan_modules(self):
        """ Shoudl be broken down into list valid files, and process files.
        """
        module_list = []
        oldpath = sys.path
        for dirent in os.listdir(str(self.moduledir)):
            filepath = os.path.join(self.moduledir,dirent)
            if os.path.isfile(filepath):
                module_name, ext = os.path.splitext(filepath) # Handles no-extension files, etc.
                if ext == '.cfg': # Important, ignore .pyc/other files.
                    import_module = ConfigParser.ConfigParser()
                    import_module.read(filepath)
                    for section in import_module.sections():
                        mod = {'name':section}
                        # TODO: This code could be more DRY
                        # Note two functions could reduce this code.
                        # Note all shoudl support double quotes.
                        if import_module.has_option(section,"language"):
                            language_raw = import_module.get(section,"language")
                            mod['language'] = language_raw.strip('"').strip()
                        if import_module.has_option(section,"path_files"):
                            path_files_raw = import_module.get(section,"path_files")
                            list_raw = path_files_raw.split(",")
                            listcleaned = []
                            for item in list_raw:
                                listcleaned.append(item.strip())
                            mod['path_files'] = listcleaned
                        if import_module.has_option(section,"path_directories"):
                            path_directories_raw = import_module.get(section,"path_directories")
                            list_raw = path_directories_raw.split(",")
                            listcleaned = []
                            for item in list_raw:
                                listcleaned.append(item.strip())
                            mod['path_directories'] = listcleaned
                        if import_module.has_option(section,"run"):
                            run_raw = import_module.get(section,"run")
                            mod['run'] = run_raw.strip()
                        if import_module.has_option(section,"check"):
                            check_raw = import_module.get(section,"check")
                            mod['check'] = check_raw.strip()
                        if import_module.has_option(section,"requirements"):
                            requirements_raw = import_module.get(section,"requirements")
                            mod['requirements'] = requirements_raw.strip()
                        if import_module.has_option(section,"description"):
                            description_raw = import_module.get(section,"description")
                            mod['description'] = description_raw.strip()
                        if import_module.has_option(section,"notes"):
                            notes_raw = import_module.get(section,"notes")
                            mod['notes'] = notes_raw.strip()
                        module_list.append(mod)
        self.module_list = module_list

    def list_modules(self):
        output = []
        for module in self.module_list:
            output.append(module.name)
        return output

    def execute_module(self,module_name,cfg,hostname):
        """ TODO: maybe refactor to use Chain-of-Responsibility Pattern
        """
        rc = True
        for module in self.module_list:
            if module_name == module['name']:
                matchingDriver = False

                if module['language'] == "bash":
                    matchingDriver = True
                    rc = execute_module_bash.execute_module(module,cfg,hostname,self.dir_mod_base_sh)
                if module['language'] == "python":
                    matchingDriver = True
                    rc = execute_module_python.execute_module(module,cfg,hostname,self.dir_mod_base_py)
                if matchingDriver == False:
                    logger.error("Module '%s' has an invalid language." % (module['name']))
                    rc = False
        return rc

    def registered(self,registered):
        """ TODO: This function and info should be merged.
        Checks a module with the name 'registered' exists.
        """
        rc = False
        for module in self.module_list:
            if module['name'] == registered:
                rc = True
        return rc

    def info(self,module_name):
        """ TODO: This function and registered should be merged.
        returns a module with the name 'module_name' exists,
        if no module exists return None.
        """
        for module in self.module_list:
            if module['name'] == module_name:
                return module
        return None

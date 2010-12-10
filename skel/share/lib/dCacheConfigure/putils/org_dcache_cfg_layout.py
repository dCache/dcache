import re
import os.path
import logging

logger = logging.getLogger("dCacheConfigure.putils.org_dcache_cfg_layout")

regdelexp = re.compile('[#=]')

class layout_util(Exception):
       def __init__(self, value):
           self.parameter = value
       def __str__(self):
           return repr(self.parameter)

def split_line_by_delimiter(regex, line):
    splitline = []
    splititr = regdelexp.finditer(line)
    lstart = 0
    for i in splititr:
        (mstart,mend) = i.span()
        if lstart != mstart:
            splitline.append(line[lstart:mstart])
        splitline.append(line[mstart:mend])
        lstart = mend
    linelen = len(line)
    if lstart != linelen:
        splitline.append(line[lstart:linelen])
    return splitline

class lmodel_exception(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)


class lmodel_top:
    def __init__(self):
        self.domains = []


class lmodel_domain:
    def __init__(self,name):
        self.name = name
        # All lines excluding defining line and service lines
        self.lines = []
        self.services = []

    def get_commented(self):
        if hasattr(self,"comment_domain"):
            return self.comment_domain
        else:
            all_commented = True
            for service in self.services:
                if not service.commented:
                    all_commented = False
                    break
            return all_commented
    def set_commented(self,commented):
        self.comment_domain = commented
        if commented:
            for service in self.services:
                if not service.commented:
                    service.commented = True


    commented = property(get_commented, set_commented)


class lmodel_service:
    def __init__(self,name):
        self.name = name
        # All lines exclugin defining line
        self.lines = []
        self.properties = {}
        self.commented = False

    def lines2properties(self):
        output = {}
        for line in self.lines:

            # Split lines by = and #
            splitline = split_line_by_delimiter(regdelexp, line)

            # Remove commented lines
            delimiter = -1
            for interator3 in range(len(splitline)):
                if splitline[interator3] == '#':
                    delimiter = interator3
                    break
            if delimiter >= 0:
                splitline = splitline[:delimiter]

            # find '=' so as to splite head and tail
            delimiter = -1
            for interator3 in range(len(splitline)):
                if splitline[interator3] == '=':
                    delimiter = interator3
                    break
            if delimiter == -1:
                continue
            head = ""
            tail = ""
            for interator3 in range(delimiter):
                head += splitline[interator3]
            for interator3 in range(delimiter + 1,len(splitline)):
                tail += splitline[interator3]
            output[head] = tail
        return output

    def properties2lines(self):
        newlines = []
        keys_done = []
        for line in self.lines:
            if self.commented:
                newlines.append("# %s" % (line))
                continue
            # Split lines by = and #
            splitline = split_line_by_delimiter(regdelexp, line)

            # Remove commented lines
            delimiter = -1
            for interator3 in range(len(splitline)):
                if splitline[interator3] == '#':
                    delimiter = interator3
            if delimiter >= 0:
                splitline = splitline[:delimiter]

            # fine '=' so as to splite head and tail
            delimiter = -1
            for interator3 in range(len(splitline)):
                if splitline[interator3] == '=':
                    delimiter = interator3
            if delimiter == -1:
                newlines.append(line)
                continue
            head = ""
            tail = ""
            for interator3 in range(delimiter):
                head += splitline[interator3]
            for interator3 in range(delimiter + 1,len(splitline)):
                tail += splitline[interator3]
            # Should we comment
            comment = False
            if not head in self.properties.keys():
                comment = True
            else:
                if str(tail) != str(self.properties[head]):
                    comment = True
                else:
                    keys_done.append(head)
            if comment:
                newlines.append("# %s" % (line))
            else:
                newlines.append(line)

        if not self.commented:
            for prop in self.properties.keys():
                if prop in keys_done:
                    continue
                newlines.append("%s=%s" % (prop,self.properties[prop]))
        return newlines



class lview:

    def __init__(self,model):
        self.model = model

    def domain_add(self,name):
        # Domains are unique
        for i in range(len(self.model.domains)):
            if self.model.domains[i].name == name:
                raise error
        self.model.domains.append(lmodel_domain(name))

    def service_add(self,domain,service):
        req_item = -1
        for i in range(len(self.model.domains)):
            if self.model.domains[i].name == domain:
                req_item = i
                break
        if req_item == -1:
            logger.critical("Domain not found")
            raise error
        self.model.domains[i].append(lmodel_service(service))


    def property_add(self,domain,service,name,key,value):
        pass



    def property_get(self,domain,service,key):
        if domain == None:
            return self.model.attribs[key]
        if not domain in self.model.child.keys():
            raise error
        if service == None:
            return self.model.child[domain].attribs[key]
        return self.model.child[domain].child[service].attribs[key]

    def attribs_list(self,domain,service):
        if domain == None:
            return self.model.attribs.keys()
        if not domain in self.model.child.keys():
            return []
        if service == None:
            return self.model.child[domain].attribs.keys()
        if not service in self.model.child[domain].child.keys():
            return []
        return self.model.child[domain].child[service].attribs.keys()

    def domain_list_by_service(self,service):
        dom_list = set([])
        for domain in self.model.child.keys():
            if service in self.model.child[domain].child.keys():
                dom_list.add(domain)
        return dom_list


    def get_domain_by_name(self,domain_name):
        for domain_itr in range(len(self.model.domains)):
            if self.model.domains[domain_itr].name == domain_name:
                return self.model.domains[domain_itr]
        return None

    def get_service_dict_by_type(self,service_type):
        output = []
        for domain_itr in range(len(self.model.domains)):
            if self.model.domains[domain_itr].commented:
                continue
            for service_itr in range(len(self.model.domains[domain_itr].services)):
                if self.model.domains[domain_itr].services[service_itr].commented:
                    continue
                if self.model.domains[domain_itr].services[service_itr].name == service_type:
                    output.append(self.model.domains[domain_itr].services[service_itr])
        return output

    def load(self, file_name):
        lmodel_top.__init__(self.model)
        fp = open(file_name, 'r')
        alllines = fp.readlines()

        regexp = re.compile('^\[.*\]$')
        blockcontainer = []
        block = []
        for line in alllines:
            striped_line = line.strip()
            if None == regexp.match(striped_line):
                block.append(striped_line)
            else:
                blockcontainer.append(block)
                block = [striped_line]
        blockcontainer.append(block)
        dom_name = None
        domainlist = []
        domblock = []
        for block in blockcontainer:
            if len(block) == 0:
                continue
            header = block[0]
            posdomname = ""
            for i in range(1,len(header)):
                if header[i] == '/' or header[i] == ']':
                    posdomname = header[1:i]
                    break
            if posdomname != dom_name:
                domainlist.append(domblock)
                domblock = [block]
                dom_name = posdomname
            else:
                domblock.append(block)
        if len(domblock) > 0:
            domainlist.append(domblock)

        for domain in domainlist:
            domainname = None
            domlines = []
            if len(domain) > 0:
                domlines = domain[0]
                if len(domlines) > 0:
                   domainname  = domlines[0].strip('[]')

            dom = lmodel_domain(domainname)
            # we dont want to include the domain definition in the list of lines
            # as this is not the domains busness.
            if len(domlines) > 1:
                dom.lines = domlines[1:]
            else:
                dom.lines = []
            if len(domain) > 1:
                expectedprefix = '[%s/' % (domainname)
                for iterator1 in range(1,len(domain)):
                    servicelines = domain[iterator1]
                    servicename = None
                    if len(servicelines) > 0 :
                        if not len(servicelines[0]) > len(expectedprefix):
                            logger.critical("Service definition to short minimum size is '%s' adn size is %s" % (len(servicelines[0]),len(expectedprefix)))
                            raise error('to short service definition')
                        servicename = servicelines[0][len(expectedprefix):].rstrip(']')
                        service = lmodel_service(servicename)
                        service.lines = servicelines[1:]
                        service.properties = service.lines2properties()
                        dom.services.append(service)
            self.model.domains.append(dom)



    def save(self, file_name):
        output = []
        for domain in self.model.domains:
            domainempty = False
            domainlines = []
            if domain.name != None:
                domainlines.append("[%s]" % (domain.name))
            for line in domain.lines:
                domainlines.append(line)
            for service in domain.services:
                servicelines = []
                servicelines.append("[%s/%s]" % (domain.name,service.name))

                for line in service.properties2lines():
                    servicelines.append(line)

                for line in servicelines:
                    # We comment lines if service is commented
                    # but dont comment if domain is commented as
                    # This will comment anyway
                    if service.commented and not domain.commented:
                        domainlines.append("# %s" % line)
                    else:
                        domainlines.append(line)
            for line in domainlines:
                if domain.commented:
                    output.append("# %s" % line)
                else:
                    output.append(line)
        fp = open(file_name, 'w')
        for line in output:
            logger.debug("layoutline=%s" % (line))
            line += '\n'
            fp.write(line)
        fp.flush()
        fp.close()
        return


class lcntrl:
    def __init__(self):
        self.model = lmodel_top()
        self.lview = lview(self.model)
    def load(self,file_name):
        self.lview.load(file_name)

    def testservices(self):
        liststuff = self.lview.domain_list_by_service('dir')

    def save(self,file_name):
        self.lview.save(file_name)
        return 0

    def domain_list_by_service(self,service):
        return self.lview.domain_list_by_service(service)
    def dommain_add(self,name):
        return self.lview.domain_add(name)
    def service_add(self,domain,name):
        return self.lview.service_add(domain,name)


    def get_service_dict_by_type(self,service_type):
        output = []
        for service in self.lview.get_service_dict_by_type(service_type):
            output.append(service.properties)
        return output

    def comment_service_dict_by_type_properties(self,service_type,properties):
        for service in self.lview.get_service_dict_by_type(service_type):
            matches = True
            for key in properties.keys():
                if not key in service.properties.keys():
                    matches = False
                else:
                    if service.properties[key] != properties[key]:
                        matches = False
            if matches:
                service.commented = True
    def create_service_dict_by_type_properties(self,domain_name,service_type,properties):
        found = False
        for service in self.lview.get_service_dict_by_type(service_type):
            matches = True
            for key in properties.keys():
                if not key in service.properties.keys():
                    matches = False
                else:
                    if service.properties[key] != properties[key]:
                        matches = False
            if matches:
                service.commented = False
                return service
        default_domain = None
        if domain_name == None:

            raise layout_util("Domain not set")
        else:
            default_domain = domain_name
        dom = self.lview.get_domain_by_name(default_domain)
        if dom == None:
            newdom = lmodel_domain(default_domain)
            self.model.domains.append(newdom)
            dom = self.lview.get_domain_by_name(default_domain)
        service = lmodel_service(service_type)
        service.properties = properties
        dom.services.append(service)

if __name__ == "__main__":

    cntrl = lcntrl()
    cntrl.load("single.conf")
    cntrl.save("fred.conf")
    cntrl.testservices()

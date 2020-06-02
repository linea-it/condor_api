class Utils():

    def parse_requirements(self, search_fields, **args):

        requirements = []
        search = []

        for arg in args:

            if len(arg.split('__')) == 2:

                requirement = arg.split('__')[0]
                condition = arg.split('__')[1]
                value = args[arg]

                if requirement and condition:
                    if condition in 'eq':
                        requirements.append(requirement + '==' + self.parse_value(value))
                    elif condition in 'gt':
                        requirements.append(requirement + '>' + value)
                    elif condition in 'lt':
                        requirements.append(requirement + '<' + value)
                    elif condition in 'gte':
                        requirements.append(requirement + '>=' + value)
                    elif condition in 'lte':
                        requirements.append(requirement + '<=' + value)
                    elif condition in 'range':
                        if len(value.split(',')) == 2:
                            requirements.append(requirement + ' ' + 'BETWEEN' + ' ' + self.parse_value(value.split(',')[0]) + ' ' + 'AND' + ' ' + self.parse_value(value.split(',')[1]) )
                    elif condition in 'contains':
                        requirements.append(requirement + ' ' + 'like' + ' ' + '"' + '%' + eval(self.parse_value(value)) + '%' + '"')

            if(arg == 'search'):
                if(args[arg] is not ''):
                    for field in search_fields:
                        search.append(field + ' like "%' + args[arg] + '%"')

        response = ''

        if(len(search) != 0 and len(requirements) != 0):
            response = '|'.join(str(e) for e in search) + '&' + '&'.join(str(e) for e in requirements)
        elif(len(search) != 0):
            response = '|'.join(str(e) for e in search)
        elif(len(requirements) != 0):
            response = '&'.join(str(e) for e in requirements)

        return response

    def parse_value(self,value):

        try:
            float(value)
            return value
        except ValueError:
            val = "\"%s\"" % value
            return val

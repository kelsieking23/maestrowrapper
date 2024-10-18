import os


class MAE:

    def __init__(self, file):
        self.file = file
        dic = self.parse_file_to_dict(self.file)
        for key, val in dic.items():
            print(key)
            print(val)
            print('\n')
            for item in dic[key]:
                print(item)
            print('\n')

    def parse_file_to_dict(self, filename):
        # this dont work lol need to fix
        data = {}
        parent_key = None
        sub_key = 'data'
        last_sub_key = None
        with open(filename, 'r') as file:
            for line in file:
                line = line.strip()
                if line.startswith('{') or line.startswith('}') or line == "":
                    continue  # Skip lines with only brackets or empty lines
                elif line.endswith('{'):
                    if parent_key is None:
                        parent_key = line.split('{')[0].strip()
                        sub_key = 'data'
                        data[parent_key] = {sub_key:[]}
                    else:
                        last_sub_key = sub_key
                        sub_key = line.split('{')[0].strip()
                        data[parent_key][sub_key] = []
                elif line.endswith('}'):
                    if sub_key == 'data':
                        parent_key = None
                        sub_key = 'data'
                        last_sub_key = None
                    else:
                        sub_key = last_sub_key
                        last_sub_key = None

                else:
                    if parent_key:
                        data[parent_key][sub_key].append(line.strip('\n'))
        return data


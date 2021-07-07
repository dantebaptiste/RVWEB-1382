import json
import logging


class VariableManager:
    """A class for managing a set of variables stored in a file."""
    full_path_to_variables_file = ''

    def __init__(self, full_path_to_file):
        """This function receives the full path for the file that stores
        the variables being managed."""
        if full_path_to_file != '':
            # the if above allows this class to be initialized with an empty
            # string, which may be useful somtimes, but then the object must be
            # re-assigned afterwards or it will not work.
            # logging.debug('Initializing instance of VariableManager()')
            self.full_path_to_variables_file = full_path_to_file
            try:
                with open(full_path_to_file, mode='r') as file_with_vars:
                    dict_with_vars = json.load(file_with_vars)
                    # this try/except is used to check if the file exists or not. However
                    # pylama does not like that the dictionary above does not get used, so adding
                    # the dummy line below for compliance with pylama.
                    dict_with_vars.clear()
            except FileNotFoundError:
                # if we are here, the file likely does not exist yet, so we create it
                with open(full_path_to_file, mode='w') as file_with_vars:
                    file_with_vars.write('{}')

    def var_set(self, var_name, var_value):
        with open(self.full_path_to_variables_file, mode='r') as file_with_vars:
            dict_with_vars = json.load(file_with_vars)
        logging.debug('Setting variable: ' + var_name + ' to -> ' + str(var_value))
        dict_with_vars[var_name] = var_value
        with open(self.full_path_to_variables_file, mode='w') as file_with_vars:
            json.dump(dict_with_vars, file_with_vars)

    def var_retrieve(self, var_name):
        with open(self.full_path_to_variables_file, mode='r') as file_with_vars:
            dict_with_vars = json.load(file_with_vars)
            try:
                return dict_with_vars[var_name]
            except Exception:
                logging.error('Unknown variable requested from VariableManager class.')

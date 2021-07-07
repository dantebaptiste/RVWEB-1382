import my_globals
import my_config
import logging
from airtable import Airtable


class MyAirtWrapper(Airtable):
    """An child class I created of the Airtable python wrapper I found on the
    interwebs. My class allows for performing some additional checks."""

    def __init__(self, str_airtable_table, timeout=my_globals.int_timeout, perform_checks=False):
        okay_to_initialize = False
        if perform_checks:
            if self.confirm_airtable_intended_base():
                okay_to_initialize = True
            else:
                logging.error("Airtable base not confirmed. Exiting...")
                exit(0)
        else:
            okay_to_initialize = True
        if okay_to_initialize:
            logging.debug('Initializing instance of MyAirtableWrapper() class')
            super().__init__(my_globals.str_AT_base, str_airtable_table,
                             my_config.airtable_api_key, timeout=timeout)

    # ------------------------ END FUNCTION ------------------------ #

    def confirm_airtable_intended_base(self):
        """A function that can be used during troubleshooting to make
         sure one is in the right environment, based on environment
         variables."""
        okay_to_continue = False
        str_base_env = self.checkWorkingInTestOrProd()

        print('¡¡¡ WARNING WARNING WARNING !!!')
        print('You are about to use airtable base:')
        print(str_base_env)
        print('Type 479 (Enter) to confirm if working in TEST.')
        print('Type 9731 (Enter) to confirm if working in PROD.')
        user_input = input('Enter confirmation number:\n')
        if user_input == '479' and str_base_env == 'TEST':
            okay_to_continue = True
        if user_input == '9731' and str_base_env == 'PROD':
            okay_to_continue = True
        return okay_to_continue

    # ------------------------ END FUNCTION ------------------------ #

    def checkWorkingInTestOrProd(self):
        """This method should only be used during troubleshooting to make
        sure environment variables are being populated as expected."""
        str_base_env = my_globals.str_AT_base_working_on
        if not (str_base_env == 'TEST' or str_base_env == 'PROD'):
            logging.error('UNKNOWN AIRTABLE BASE - NEITHER PROD NOR TEST. Exiting...')
            exit(0)
        return str_base_env
    # ------------------------ END FUNCTION ------------------------ #

import logging
import json
import urllib3
import certifi
from http.cookies import SimpleCookie
import my_globals
import my_config
from class_variable_manager import VariableManager


class AuthenticateRVwebsite:
    """This class provides headers and cookies necessary for making
    authenticated requests to the RV website."""
    file_path_to_store_auth_data = my_globals.str_fullfilepath_rv_website_authentication_data
    __str_logged_in = 'logged_in'
    __str_dct_cookies = 'dct_cookies'
    __str_dct_headers = 'dct_headers'
    __auth_url = 'https://www.realvision.com/rv/api/users?profile=login'

    def __init__(self):
        """Initialize an object for RV website Authentication."""

        # the variable manager is used to save and load the authentication
        # details to and from disk
        self.var_mgr = VariableManager(self.file_path_to_store_auth_data)
        self.logged_in = self.var_mgr.var_retrieve(self.__str_logged_in)
        self.dct_cookies = self.var_mgr.var_retrieve(self.__str_dct_cookies)
        self.dct_headers = self.var_mgr.var_retrieve(self.__str_dct_headers)

    # ------------------------ END FUNCTION ------------------------ #

    def login_rv_website(self):
        """Login to the RV website.
        This method uses urllib3 to post an authentication request to the appropriate RV API
        url. It then uses the response to extract the authentication token in the form of a cookie.
        Then, after this method has successfully run, the code using the class can call
        on the public dct_cookies and dct_headers variables to make requests
        to RV API pages in an authenticated state."""

        # NOTE: the headers in the line below are only the headers passed to the initial authentication request.
        # They are not the headers that end-up getting stored in the dct_headers public varialbe of this class.
        # Those headers are loaded from disk. Those headers were 'compiled' by seeing what headers
        # Selenium/Firefox typically use, and they seem to work better for certain pages of the API
        # (for example json_transcipt video pages, where using the headers below caused a 406
        # 'Not Acceptable' type error.)
        initial_request_headers = {"accept": "application/vnd.api+json", "content-type": "application/vnd.api+json"}
        initial_payload = json.dumps({"data": {"attributes": {"email": my_config.rv_u, "password": my_config.rv_p}}})
        self.pool = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where(),
                                        timeout=float(my_globals.int_timeout))

        try:
            response = self.pool.urlopen('POST', self.__auth_url, headers=initial_request_headers, body=initial_payload)
            if response.status == 200:
                logging.info('Successfully logged in to RV website.')
                self.logged_in = True
                thecookies = response.headers['set-cookie']
                self.dct_cookies = self.__make_cookies_dict_from_string(thecookies)
                self.var_mgr.var_set(self.__str_logged_in, self.logged_in)
                self.var_mgr.var_set(self.__str_dct_cookies, self.dct_cookies)
                self.var_mgr.var_set(self.__str_dct_headers, self.dct_headers)
            else:
                raise Exception('Non-200 status when attempting to authenticate with the RV website.')
        except Exception as e:
            logging.warning("Problem during function 'login_rv_website'."
                            " The Exception was: " + repr(e))

    # ------------------------ END FUNCTION ------------------------ #

    def logout_rv_website(self):
        """Logout of the RV website."""
        if self.logged_in:
            try:
                # logout by going to the logout page
                response = self.pool.request('GET', 'https://www.realvision.com/rv/logout')
                if response.status == 200:
                    logging.info('Logged out of RV website.')
                    self.logged_in = False
                    self.var_mgr.var_set(self.__str_logged_in, self.logged_in)
                    self.dct_cookies = {}
                    self.var_mgr.var_set(self.__str_dct_cookies, self.dct_cookies)
                else:
                    raise Exception('Non-200 status when attempting to logout of RV website.')
            except Exception as e:
                logging.warning("Problem during function 'logout_rv_website'."
                                " The Exception was: " + repr(e))
        else:
            logging.warning('logout_rv_website function was called when the current logged-in state was not'
                            ' set to True.')

    # ------------------------ END FUNCTION ------------------------ #

    def __make_cookies_dict_from_string(self, str_cookies):
        """Given cookies as a string, construct and return a dictionary."""
        dct_cookies = {}
        cookies = SimpleCookie()
        cookies.load(str_cookies)

        # Even though SimpleCookie is dictionary-like, it internally uses a Morsel object
        # which is incompatible with requests. Manually construct a dictionary instead.
        for key, morsel in cookies.items():
            dct_cookies[key] = morsel.value
        return dct_cookies
    # ------------------------ END FUNCTION ------------------------ #

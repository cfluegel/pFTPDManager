# -*- coding: utf-8 -*-


from datetime import date, datetime, timedelta
import os.path
import sys

import mysql.connector
from mysql.connector import errorcode
from hashlib import md5

# mainly for Debugging
import pprint

# PW Generation
from random import randint, choice
import string 




class pFTPDManager:
    """ pFTPDManager - Class to manage database backend of pureFTPD Server 
    
    Database Structure: 
        - User          varchar(255)    (Not Null)
        - Password      varchar(255)    (Not Null)
        - Uid           int             (Database Default 1001)
        - Gid           int             (Database Default 1001)
        - Dir           varchar(255)    (Not Null)
        - RequestedBy   varchar(100)    (Null) 
        - UsableUntil   date            (Not Null)              Maybe rename it to ExpirationDate
    
    Planned new fields:
        - Active        int             (Database Default 1)
            It gives the option to deactivate an account before it expires and it will not be necessary to 
            modify the expiraten date 

    """
    __default_lifetime = "7"                     # days
    __default_base_directory = r'/srv/ftp'       # seems a resonable default ?!

    __dbhost = None
    __dbuser = None
    __dbpass = None
    __dbname = None

    __dbconnection = None
    __dbcursor = None

    # Will be a list
    __ftpusers = list()

    def __init__(self, dbhost=None, dbuser=None, dbpass=None, dbname=None):
        if not dbhost or not dbuser or not dbpass or not dbname:
            raise ValueError("Database parameters are missing")

        self.__dbhost = dbhost
        self.__dbuser = dbuser
        self.__dbpass = dbpass
        self.__dbname = dbname

        self.db_connect()

        if not self.__dbconnection:
            raise ConnectionError("Connection failed")


    def __del__(self):
        self.db_disconnect();

    def __retrieve_ftpusers(self):
        self.__dbcursor = self.__dbconnection.cursor()
        self.__dbcursor.execute("SELECT User FROM users;")
        for row in self.__dbcursor.fetchall():
            self.__ftpusers.append(row[0])

        self.__dbcursor.close()

    def __check_date_format(self,DateString=None):
        """ Checks the format of the supplied date (ISO 8601)"""
        if not DateString:
            return False

        if not isinstance(DateString, (str)):
            return False

        if len(DateString) != 10:
            return False

        if DateString[4] != '-' or DateString[7] != '-':
            return False

        return True

    def db_disconnect(self):
        try:
            self.__dbcursor.close()
        except (AttributeError, ReferenceError):
            ## TODO: Just ignore errors for now.
            pass

        try:
            self.__dbconnection.close()
        except AttributeError:
            ## TODO: Just ignore errors for now.
            pass

    def db_connect(self):
        try:
            self.__dbconnection = mysql.connector.connect(host=self.__dbhost,
                                                         user=self.__dbuser,
                                                         passwd=self.__dbpass,
                                                         db=self.__dbname)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                raise ConnectionRefusedError("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                raise ConnectionError("Database does not exist")
            else:
                raise ConnectionError("Unknown Error") 
        except (mysql.connector.errors.DatabaseError, mysql.connector.MySQLInterfaceError):
            raise ConnectionError("Can't connect to server")

    @staticmethod
    def generate_password(length=13):
        characters = string.ascii_letters + string.digits
        return "".join(choice(characters) for x in range(randint(length, length)))
    
    def username_exists(self,username=None):
        """ Checks if the users exists already """
        if not username:
            raise ValueError("Username or Password is missing!")
        # Update user list
        self.__retrieve_ftpusers()

        if username in self.__ftpusers:
            return True
        return False

    def list_accounts(self,filter='ACTIVE'):
        self.__dbcursor = self.__dbconnection.cursor()

        # TODO: Modify the following to use the flag, after the database Scheme has been changed
        if filter == "ACTIVE":
          sql_query = "SELECT User,Dir,UsableUntil,RequestedBy FROM users WHERE CURRENT_DATE() < UsableUntil;"
        elif filter == "EXPIRED":
            sql_query = "SELECT User,Dir,UsableUntil,RequestedBy FROM users WHERE CURRENT_DATE() > UsableUntil;"
        elif filter == "ALL":
            sql_query = "SELECT User,Dir,UsableUntil,RequestedBy FROM users;"

        self.__dbcursor.execute(sql_query)
        results = self.__dbcursor.fetchall()

        self.__dbcursor.close()

        if len(results) > 0:
            return results

        return None

    def create_account(self,username=None,password=None,lifetime=None,requestedby=None):
        """ Creates a new virtual ftp account """
        if not username or not password:
            raise ValueError("Username or Password is missing!")

        # Convert password to a md5 hexdigest
        password = md5(password.encode("utf-8")).hexdigest()

        if not lifetime:
            until = datetime.now().date() + timedelta(days=self.__default_lifetime+1)

        if isinstance(lifetime, (int)):
            until = datetime.now().date() + timedelta(days=lifetime+1)
        if isinstance(lifetime, (str)):
            if not self.__check_date_format(lifetime):
                raise ValueError("Supplied Date is malformed!")
            until = datetime.strptime(lifetime,"%Y-%m-%d")

        # funktioniert nur auf Linux sauber, Windows baut hier sein Trenner ein ...
        # user_directory = os.path.join(self.__default_base_directory,username)
        user_directory = "{}/{}".format(self.__default_base_directory,username)

        if self.username_exists(username):
            raise LookupError("Users already exists")

        # Create DB Cursor
        self.__dbcursor = self.__dbconnection.cursor()

        if requestedby:
          insert_query = "INSERT INTO users (User,Password,Dir,UsableUntil,RequestedBy) VALUES (%s,%s,%s,%s,%s);"
        else: 
          insert_query = "INSERT INTO users (User,Password,Dir,UsableUntil) VALUES (%s,%s,%s,%s);"

        self.__dbcursor.execute(insert_query, (username,password,user_directory,until,requestedby))
        self.__dbconnection.commit()

        self.__dbcursor.close()

        # TODO: Check if the SQL INSERT Statement was successful
        return True

    def delete_account_files(self,username=None):
        pass

    def disable_account(self, username=None):
        if not username:
           raise ValueError("Username is missing!")

        if not self.username_exists(username):
            raise LookupError("{} not found".format(username))

        # TODO: Change the SQL Statement after the database scheme has been changed. (ActiveFlag)
        update_sql = "UPDATE users SET UsableUntil = '1983-03-22' WHERE user = %s;"

        self.__dbcursor = self.__dbconnection.cursor()
        self.__dbcursor.execute(update_sql,(username,))
        self.__dbconnection.commit()

        self.__dbcursor.close()

        return self.is_account_disabled(username)

    def is_account_disabled(self,username=None):
        if not username:
           raise ValueError("Username is missing!")

        if not self.username_exists(username):
            raise LookupError("{} not found".format(username))

        user_deactivated_sql = "SELECT User FROM users WHERE Active == 0;"  

        self.__dbcursor.execute(user_deactivated_sql)
        results = self.__dbcursor.fetchall()
        self.__dbcursor.close()

        if len(results) == 1:
            return True
        if len(results) > 1: 
            raise LookupError("Search returned more than one result. Something seems fishy")
        if len(results) == 0:
            return False

    def renew_account(self,username=None,new_lifetime=None):
        if not username or not new_lifetime:
            raise ValueError("Username or (new) Lifetime is missing!")

        if not new_lifetime:
            new_until = datetime.now().date() + timedelta(days=self.__default_lifetime+1)

        if isinstance(new_lifetime, (int)):
            new_until = datetime.now().date() + timedelta(days=new_lifetime+1)
        if isinstance(new_lifetime, (str)):
            if not self.__check_date_format(new_lifetime):
                raise ValueError("Supplied Date is malformed!")

            new_until = datetime.strptime(new_lifetime,"%Y-%m-%d")

        if not self.username_exists(username):
            raise LookupError("{} not found".format(username))

        update_sql = "UPDATE users SET UsableUntil = %s WHERE user = %s;"

        self.__dbcursor = self.__dbconnection.cursor()
        self.__dbcursor.execute(update_sql,(new_until,username))
        self.__dbconnection.commit()

        self.__dbcursor.close()

        # TODO: Check if the SQL INSERT Statement was successful
        return True

    def get_requester(self,username=None):
        if not username:
          raise ValueError("Username is missing!") 

        sql_query = "SELECT RequestedBy FROM users WHERE user = %s;"

        self.__dbcursor = self.__dbconnection.cursor()
        self.__dbcursor.execute(sql_query, (username,))
        results = self.__dbcursor.fetchone()

        self.__dbcursor.close()

        return results

if __name__ == "__main__":
    pass

    #### I need for some late time 
    # from getpass import getpass
    # # dbhost=None, dbuser=None, dbpass=None, dbname=None

    # try:
    #     ftp_connection = pFTPDManager(dbhost="localhost",
    #                                 dbuser="pureftpd",
    #                                 dbpass=getpass(),
    #                                 dbname="pureftpd")
    # except (ConnectionError, Exception):
    #     print("No connection...")
    #     sys.exit(1)


    # if ftp_connection.username_exists("cfluesdfgeltest345"):
    #     print("Ja")
    # else:
    #     print("Nein")

    # pprint.pprint(ftp_connection.list_accounts('ALL'))

    # pprint.pprint(ftp_connection.get_requester("ftp-22c69afa82")[0])



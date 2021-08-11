# -*- coding: utf-8 -*-


from datetime import date, datetime, timedelta

import os.path
import sys

import mysql.connector
from mysql.connector import errorcode
from hashlib import md5
import hashlib 

# mainly for Debugging
import pprint

# PW Generation
from random import randint, choice, random 
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
    
    Planned new database fields:
        - Active        int             (Database Default 1)
            It gives the option to deactivate an account before it expires and it will not be necessary to 
            modify the expiraten date 
        - CreatedOn     date            (Not Null / Database Default to the CURRENT_DATA())
            As the date for UsableUntil can be changed till the end of the universe, it does not give us 
            precise information about when an Account has been created 
        - LastRenewOn   date            (Null)
            This is probably just a dump idea. But maybe it is interesting to know if the account has been 
            renewed and when this happend. 
        - RenewCounter  int             (Database Defaults to 0)
            So we know how active the account is used. Have to be increased every time a account is renewed

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

    def init_database_tables(self):
        self.__dbcursor = self.__dbconnection.cursor()
        create_query = """CREATE TABLE users (
                            User varchar(255) NOT NULL, 
                            Password varchar(255) NOT NULL,
                            Uid int(11) NOT NULL DEFAULT '1001',
                            Gid int(11) NOT NULL DEFAULT '1001',
                            Dir varchar(255) NOT NULL,
                            RequestedBy varchar(255) DEFAULT NULL,
                            RequestedOn date NOT NULL,
                            Active int(11) NOT NULL DEFAULT 1,
                            ExpirationDate date NOT NULL,
                            RenowedOn date DEFAULT NULL,
                            RenowedCounter int(11) NOT NULL DEFAULT 0,
                            PRIMARY KEY (User)
                        )"""
        
        self.__dbcursor.execute(create_query)
        self.__dbconnection.commit()
        self.__dbcursor.close()

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
    
    @staticmethod
    def generate_username(seed=None, length=13):
        if not seed: 
            raise ValueError("Seed is missing")

        sha1 = hashlib.sha1(seed.encode("utf-8") + 
                            str(random()).encode("utf-8") + 
                            datetime.utcnow().strftime("%s").encode("utf-8"))
        
        return "{}{}".format("ftp-", sha1.hexdigest()[0:10])

    def username_exists(self,username=None):
        """ Checks if the users exists already """
        if not username:
            raise ValueError("Username or Password is missing!")
        # Update user list
        self.__retrieve_ftpusers()

        # TODO: What is more efficient? Check the memory or make a Database Query? 
        if username in self.__ftpusers:
            return True
        return False

    def list_accounts(self,filter='ACTIVE'):
        self.__dbcursor = self.__dbconnection.cursor()

        if filter == "ACTIVE":
            sql_query = "SELECT User,Dir,UsableUntil,RequestedBy,RequestedOn,RenowedOn,RenowedCounter FROM users WHERE Active = 1;"
        elif filter == "EXPIRED":
            sql_query = "SELECT User,Dir,UsableUntil,RequestedBy,RequestedOn,RenowedOn,RenowedCounter FROM users WHERE Active = 0;"
        elif filter == "ALL":
            sql_query = "SELECT User,Dir,UsableUntil,RequestedBy,RequestedOn,RenowedOn,RenowedCounter FROM users;"

        self.__dbcursor.execute(sql_query)
        results = self.__dbcursor.fetchall()

        self.__dbcursor.close()

        if len(results) > 0:
            return results

        return None

    def list_accounts_that_expire_soon(self,days_to_expire=3):
        # TODO: Return a list of all accounts that will expire in (amount) days.         
        pass 

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
        # TODO: Decide if this is a function the core library needs to support directly. I may be better to just 
        #       return the list of expired accounts (maybe with a filter e.g. accounts with an expiration date 
        #       older than 180 days) 

        pass

    def deactivate_account(self, username=None):
        if not username:
           raise ValueError("Username is missing!")

        if not self.username_exists(username):
            raise LookupError("{} not found".format(username))

        # TODO: Change the SQL Statement after the database scheme has been changed. (ActiveFlag)
        update_sql = "UPDATE users SET Active = '0' WHERE user = %s;"

        self.__dbcursor = self.__dbconnection.cursor()
        self.__dbcursor.execute(update_sql,(username,))
        self.__dbconnection.commit()
        self.__dbcursor.close()

        return self.is_account_deactivated(username)

    def activate_account(self, username=None):
        if not username:
           raise ValueError("Username is missing!")

        if not self.username_exists(username):
            raise LookupError("{} not found".format(username))

        # TODO: Change the SQL Statement after the database scheme has been changed. (ActiveFlag)
        update_sql = "UPDATE users SET Active = '1' WHERE user = %s;"

        self.__dbcursor = self.__dbconnection.cursor()
        self.__dbcursor.execute(update_sql,(username,))
        self.__dbconnection.commit()
        self.__dbcursor.close()

        return self.is_account_deactivated(username)

    def is_account_deactivated(self,username=None):
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

    def is_account_expired(self, username=None):
        if not username:
           raise ValueError("Username is missing!")

        if not self.username_exists(username):
            raise LookupError("{} not found".format(username))

        user_deactivated_sql = "SELECT User FROM users WHERE CURRENT_DATE() > UsableUntil and User = %s;"  

        self.__dbcursor.execute(user_deactivated_sql,(username,))
        results = self.__dbcursor.fetchall()
        self.__dbcursor.close()

        if len(results) == 1:
            return True
        if len(results) > 1: 
            raise LookupError("Search returned more than one result. Something seems fishy")
        if len(results) == 0:
            return False

    def renew_account(self,username=None,new_lifetime=None):
        # TODO: What about deactivated accounts, do we allow the renewal of those accounts? 
        #       Administratively deactivated for a reason? 
        if not username or not new_lifetime:
            raise ValueError("Username or (new) expiration date is missing!")

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

        # TODO: We just silently activate an account when a renewal request is done 
        if self.is_account_deactivated(username): 
            self.activate_account(username)

        update_sql = "UPDATE users SET UsableUntil = %s, RenewCounter = (RenewCounter + 1), RenowedOn = CURRENT_DATE() WHERE user = %s;"

        self.__dbcursor = self.__dbconnection.cursor()
        self.__dbcursor.execute(update_sql,(new_until,username))
        self.__dbconnection.commit()
        self.__dbcursor.close()

        # TODO: Check if the SQL INSERT Statement was successful
        return True

    def get_requester(self,username=None):
        if not username:
          raise ValueError("Username is missing!") 

        sql_query = "SELECT RequestedBy,RequestedOn FROM users WHERE user = %s;"

        self.__dbcursor = self.__dbconnection.cursor()
        self.__dbcursor.execute(sql_query, (username,))
        results = self.__dbcursor.fetchone()

        self.__dbcursor.close()

        return results

if __name__ == "__main__":
    pass

    #### I need for some late time 
    # TODO: Use pytest to check the functionallity of this class 
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



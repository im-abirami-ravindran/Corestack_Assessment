from pymongo import MongoClient
from datetime import datetime
import pymongo
import logging

logging.basicConfig(filename='app.log',
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

def get_hosts():
    """
       This Function is made to copy data from one Collection to 
       another it could be in the same Db or different DB 
    """
    conn = MongoClient()
    if conn is None: #here we are checking if mongo is making an connection
        return logging.critical('Connection unsuccessful')  #Error thrown and function will end
    else: 
       
        logging.info('User connected to Mongo Client Successfully') # Logs that connections is a success
        try:
            dbnames = conn.list_database_names() #we are checking if the DB entered exist in the list of DB in the system

            sourcedb=input('Enter the source database:') #User input of database name
            if sourcedb == '' : #if user doesnt enter any name 
                return logging.error("No value entered in Source Db")  # error thrown and function exited
            if sourcedb in dbnames: # Checking if the db exists in the list of Db in the system
                logging.info('Source Database Exists') #Logs that DB exists
            # if sourcedb  not in dbnames:
            else:  
                return logging.critical('Source Database does not exist')  # error thrown that the DB doesnt exists and function exited
            
            sourcecoll=input('Enter the source collection:')  #User input of Collection name
            x=conn[sourcedb]    
            collectionames=x.list_collection_names()    #all the collection in that DB

            if sourcecoll == '': #if user doesnt enter any name 
                return logging.error("No value entered for source collection") # error thrown and function exited
            if sourcecoll in collectionames: # Checking if the db exists in the list of Db in the system
                logging.info('Source Collection Exists') # logging that Collection Exists
            else:
                return logging.critical('Source Collection Unavailable') # logging that Collection doesnt exists and function exited

            # logging.info('All user inputs done')
    
            targetdb=input('Enter the target database:') #user imput for the target DB

            if targetdb == '': #Checking if user doesnt enter any name
                return logging.error("No value entered for Target DB") # error thrown and function exited

            if targetdb  in dbnames: # Checking if the db exists in the list of Db in the system
                logging.info('Target Database Exists') #Logs that DB exists
            else:
                logging.warning('Target Database doesnt exist, Created user entered Db') # Logs the DB doesnt exist

            targetcoll=input('Enter the target collection:') #User input of Collection name
            y=x[sourcecoll] 
            if targetcoll =='':  #if user doesnt enter any name 
                return logging.error('No value entered for Target Collection')  # error thrown and function exited

            if  targetcoll in collectionames: # Checking if the Collection exists in the list of Collection in the system
                logging.info('Target Collection exists') #Logs Collection exists
            else:
                logging.warning('Target Collection doesnt exist,Created user entered Db') # Logs the DB doesnt exist

            a=conn[targetdb] #coonecting the target Db and storing in a var
            b=a[targetcoll]#from the DB finding the collection and storing it in another variable

            logging.info("All userinputs successfully entered") # logs everything was a success

            hosts_obj = y.find() #taking all the data from the source collection
            logging.info("Time stamp berfore inserting") #logging the time before starting
            for x in hosts_obj: # inserting into the target collection
                b.insert_many(hosts_obj)
            logging.info("Time stamp after inserting") #logging the time after the insert

            # print('Out of the loop')
            logging.info('Successfully Copied') #Log of Completion
        except:
            print("No hosts found") 
        finally:
            print("============END==============")

get_hosts()

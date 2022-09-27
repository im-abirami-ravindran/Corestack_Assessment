from pymongo import MongoClient
import logging

logging.basicConfig(filename='app.log',
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')
def trdbcpy():
    try:
        '''
        This function enables to get userinput of Target DB and Collection,Check if it exists or not.
        If not creates one and Copies the collection data from source.
        '''
        global targetdb,targetcoll 
        targetdb=input('Enter the target database:') 
        if targetdb == '':
            return logging.error("No value entered for Target DB") 

        if targetdb  in dbnames:  
            logging.info('Target Database Exists') 
        else:
            logging.warning('Target Database doesnt exist, Created user entered Db') 
        targetcoll=input('Enter the target collection:') 
         
        if targetcoll =='':  
            return logging.error('No value entered for Target Collection')  

        if  targetcoll in collectionames: 
            logging.info('Target Collection exists') 
        else:
            logging.warning('Target Collection doesnt exist,Created user entered Db') 

        a=conn[targetdb] 
        b=a[targetcoll]

        logging.info("All userinputs successfully entered") 
        x=conn[sourcedb]
        y=x[sourcecoll]
        hosts_obj = y.find() 
        logging.info("Time stamp berfore inserting") 
        b.insert_many(hosts_obj)
        # for i in hosts_obj: 
            # b.insert_one(i)
        logging.info("Time stamp after inserting") 

        logging.info('Successfully Copied') 
    except: 
        print("Exception in tgt DB")
    finally:
        print("===================END=====================")

def srdbcpy():
    logging.info('User connected to Mongo Client Successfully') # Logs that connections is a success
    
    try:
        '''
        Gets the user input for Source DB and collection.
        Checks if the entered db and collection is present or not,
        If not throws error and stops the script.
        '''
        global dbnames,collectionames
        dbnames = conn.list_database_names() 
        global sourcedb,sourcecoll
        sourcedb=input('Enter the source database:') 
        if sourcedb == '' :  
            return logging.error("No value entered in Source Db")  
        if sourcedb in dbnames: 
            logging.info('Source Database Exists') 
        else:  
            return logging.critical('Source Database does not exist')  
      
        sourcecoll=input('Enter the source collection:')  
        x=conn[sourcedb]    
        collectionames=x.list_collection_names()    

        if sourcecoll == '': 
            return logging.error("No value entered for source collection") 
        if sourcecoll in collectionames: 
            logging.info('Source Collection Exists') 
        else:
            return logging.critical('Source Collection Unavailable') 
        trdbcpy()

    except:
        print("Source DB Error")


def get_hosts():
    """
       This Function is made to copy data from one Collection to 
       another it could be in the same Db or different DB 
    """
    global conn
    conn = MongoClient()
    if conn is None: #here we are checking if mongo is making an connection
        return logging.critical('Connection unsuccessful')  #Error thrown and function will end
    else: 
        srdbcpy()
            # logging.info('All user inputs done')

get_hosts()




    

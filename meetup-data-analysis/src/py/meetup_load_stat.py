import sys
from meetup_mysql_load import MySQLUpdate



def main(config):
    print 'main() method'
    mySQLUpdate = MySQLUpdate(config)
    mySQLUpdate.run(config)




if __name__ == "__main__":

    config = {
	"db_config" : {
		"host": "108.161.128.86",
		"port": 3306,
		"user": "meetup",
		"passwd": "passw0rd",
		"db": "meetup"
	    }
    }

    table_name = sys.arg[1]
    file_name = sys.arg[2]

    config['table_name'] = table_name
    config['file_name'] = file_name
    main(config)

import meetup_utilities as utils
import logging

class MySQLUpdate(object):
    '''
    classdocs
    '''

    def __init__(self, config):
        """
        Constructor
        """
        self.config = config
        self.db_connection = utils.get_db_connection(self.config['db_config'])


        self.insert_stmt = "INSERT INTO %s " % (self.config['table_name'])
        self.insert_stmt += ' VALUES (%s)'

    def update_stats(self, stats_update_sql, stat_file):
        # Update statistics into table
        logging.info("query to be executed : " + stats_update_sql)
        cursor = self.db_connection.cursor()
        updated_stats = 0

        with open(stat_file, 'r') as f:
            this_line = f.read().rstrip('\n')

            # logging.info("this_line: " + str(this_line))

            # Insert each line which has values in csv
            final_query = stats_update_sql % this_line
            logging.info(final_query)
            cursor.execute(final_query)
            updated_stats += 1

        self.db_connection.commit()
        logging.info("Changes committed to database - updated " + str(updated_stats) + " stats.")


    def run(self):
        self.update_stats(self.insert_stmt, self.config['file_name'])
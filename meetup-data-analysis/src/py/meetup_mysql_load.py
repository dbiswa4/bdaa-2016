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
        print '\nPort:',self.config['db_config']['port']
        print '\nDatabase:', self.config['db_config']['db']
        self.db_connection = utils.get_db_connection(self.config['db_config'])


        self.insert_stmt = "INSERT INTO %s " % (self.config['table_name'])
        self.insert_stmt += ' VALUES (%s)'

    def update_stats(self, stats_update_sql, stat_file):
        # Update statistics into table
        print "query to be executed : " + stats_update_sql
        cursor = self.db_connection.cursor()
        updated_stats = 0

        f =open(stat_file, 'r')
        for line in iter(f):
            this_line = line.rstrip('\n')

            # print "this_line: " + str(this_line)

            # Insert each line which has values in csv
            final_query = stats_update_sql % this_line
            #print 'final query : ', final_query
            cursor.execute(final_query)
            updated_stats += 1

        self.db_connection.commit()
        print "Changes committed to database - updated " + str(updated_stats) + " stats."


    def run(self):
        self.update_stats(self.insert_stmt, self.config['file_name'])
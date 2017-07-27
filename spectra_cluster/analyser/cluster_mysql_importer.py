"""
This analyser import clusters from a .clustering file 
"""

from . import common
import operator 
import pymysql.cursors
import os,sys
import re


class ClusterSqliteImporter(common.AbstractAnalyser):
    """
    This tool compare two cluster lists 
    and give the statistics between them.

    Result
    ------
    The results are stored in ::tableList:: as a list of
    tables. Each table represents a statistics information. 

    TODO: 
    """
    def __init__(self):
        """
        Initialised a new ClusterListsComparer analyser.

        :return:
        """
        super().__init__()
        self.min_size = 2 # set default minium size 2
        self.file_index = 0
        self.mysql_host = "localhost"
        self.table_name = "cluster_table_temp1"
        self.table = None 
        self.over_write_table = False
        self.projects = set() 
       
        # intermediate data structures
        self.cluster_list = [] 

    def connect_and_check(self):
        #build the connection
        self.connection = pymysql.connect(host=self.mysql_host,
                                          user='pride_cluster_t',
                                          password='pride123',
                                          db='pride_cluster_t',
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        print("Opened database successfully");

        #deal with the table name
        self.check_table()
 
    def check_table(self):
        """
        Check the table name exists or not,
        create it if not exists,
        ask user if they want to overwrite the table if exists.
        """
        table_exists = None
        create_new = None

        tb_exists = "SHOW TABLES LIKE '" + self.table_name + "';"
        # create a table
        tb_create = "CREATE TABLE `" + self.table_name + "` ("                     + \
                        "id int(15) NOT NULL AUTO_INCREMENT,"    + \
                        "cluster_id varchar(100) COLLATE utf8_bin NOT NULL,"    + \
                        "cluster_ratio float NOT NULL,"    + \
                        "n_spec int(10) NOT NULL,"    + \
                        "n_id_spec int (10) NOT NULL,"    + \
                        "n_unid_spec int(10) NOT NULL,"    + \
                        "PRIMARY KEY (id)" + ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
        tb_create_spec = "CREATE TABLE `" + self.table_name + "_spec` ("                     + \
                        "id int(15) NOT NULL AUTO_INCREMENT,"    + \
                        "spectrum_title varchar(200) COLLATE utf8_bin NOT NULL,"+ \
                        "spec_prj_id varchar(10) COLLATE utf8_bin NOT NULL,"   + \
                        "is_spec_identified TINYINT(1) NOT NULL,"   + \
                        "cluster_fk int(15) NOT NULL,"   + \
                        "PRIMARY KEY (id)," + "CONSTRAINT FK_" +self.table_name + "_cluster_spec FOREIGN KEY (cluster_fk) REFERENCES `" +self.table_name + "` (id)" +\
                        ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
        tb_create_prjs = "CREATE TABLE `" + self.table_name + "_projects` ("                     + \
                        "id int(10) NOT NULL AUTO_INCREMENT,"    + \
                        "project_id varchar(10) COLLATE utf8_bin NOT NULL,"   + \
                        "PRIMARY KEY (id)" + ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
        print(tb_create)
        print(tb_create_spec)
        print(tb_create_prjs)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(tb_exists)
                result = cursor.fetchone()
                self.connection.commit()
                if result != None :
                    table_exists = True
       #except IOError as e:
                else:
                    print("Table does not exists")
                    table_exists = False
                    create_new = True
                
                if table_exists and not self.over_write_table: 
                    print("The table" + self.table_name + "is already exists, do you really want to overwrite it?")
                    answer = input("please input yes | no:  ")
                    while(answer != 'yes' and answer != 'no'):
                        answer = input("please input yes | no:")
                    if answer == 'no':
                        print("Going to exit.")
                        sys.exit(0)
                    else:
                        create_new = True 

                if self.over_write_table or create_new :
                    if table_exists:
                        print("Start droping the tables")
                        cursor.execute("DROP TABLE IF EXISTS `" + self.table_name + "_spec`;")
                        cursor.execute("DROP TABLE IF EXISTS `" + self.table_name + "_projects`;")
                        cursor.execute("DROP TABLE IF EXISTS `" + self.table_name + "`;")
                    print("Start creating table " + self.table_name)
                    cursor.execute(tb_create)
                    cursor.execute(tb_create_prjs)
                    cursor.execute(tb_create_spec)
                    self.connection.commit()
        finally:
            print ("checked table")
            #self.connection.close()

    def process_cluster(self, cluster):
        """
        Add the clusters into cluster list,
	    the projectID, assay file name and spectrum index has been extracted from the spectrum title         
        
        :param cluster: the cluster to process
        :return:
        """
        if self._ignore_cluster(cluster):
            return

        self.cluster_list.append(cluster)

    def get_project_id(self, title):
        matchObj = re.match( r'.*?(P[XR]D\d{6}).*', title)
        if matchObj:
#            print("got match" + matchObj.group(1))
            return matchObj.group(1)
        else:
            print("No PRD000000 or PXD000000 be found in the title" + title)
            return None
        
    def import_projects(self):
        with self.connection.cursor() as cursor:
            for project_id in self.projects:
                insert_sql = "INSERT INTO `" + self.table_name + "_projects`" \
                             "(project_id)" + \
                             "VALUES" + \
                             "('" + project_id + "');"
                cursor.execute(insert_sql)        
        self.connection.commit()
 
    
    
    
    def import_afile(self):
        """
        import the cluster list  of a file to mysql 

        self.id = cluster_id
        self.precursor_mz = precursor_mz
        self.consensus_mz = consensus_mz
        self.consensus_intens = consensus_intens
        self.n_spectra = len(self._spectra)
        self.identified_spectra = 0
        self.unidentified_spectra = 0
        
        spectrum:
        self.title = title
        self.precursor_mz = precursor_mz
        self.charge = charge
        self.taxids = frozenset(taxids)
        project_id
        """
        try:
            with self.connection.cursor() as cursor:
                for cluster in self.cluster_list:
                    spectra = cluster.get_spectra()
                    insert_sql = "INSERT INTO `" + self.table_name + "`" \
                            "(cluster_id, cluster_ratio, n_spec, n_id_spec, n_unid_spec)" + \
                            "VALUES" + \
                            "('" + cluster.id + "', '" + str(cluster.max_il_ratio) + "', '" + str(cluster.n_spectra) + "', '" + str(cluster.identified_spectra) + "', '" + str(cluster.unidentified_spectra) + "');"
                    cursor.execute(insert_sql)        
                    cluster_key = cursor.lastrowid
                    for spectrum in spectra:
                        project_id = self.get_project_id(spectrum.title)
                        self.projects.add(project_id)
                        insert_sql2 = "INSERT INTO `" + self.table_name + "_spec`" \
                            "(spectrum_title, spec_prj_id, is_spec_identified, cluster_fk)" + \
                            "VALUES" + \
                            "('" + spectrum.title + "', '" + project_id + "', '" + str(int(spectrum.is_identified())) + "', '" + str(cluster_key) +"');"
                        if spectrum.title == None or len(spectrum.title)<1:
                            print("Wrong spectrum title: " + spectrum.title)
                        cursor.execute(insert_sql2)        
            self.connection.commit()
        except Exception as ex:
            print(ex.message)
        finally:
            print("inserted a cluster list in to table")

    def clear(self):
        """
        clear the cluster list for a new file
        """
        self.cluster_list = [] 



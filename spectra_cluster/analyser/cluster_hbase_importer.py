"""
This analyser import clusters in a .clustering file 
"""

from . import common
import operator 
import happybase
import os,sys
import re


class ClusterHbaseImporter(common.AbstractAnalyser):
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
        self.hbase_host = "localhost"
        self.table_name = "cluster_table_temp1"
        self.table = None 
       
        # intermediate data structures
        self.cluster_list = [] 

    def connect_and_check(self):
        #build the connection
        self.connection = happybase.Connection(self.hbase_host, autoconnect=False)
        self.connection.open()
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
        try:
            table = self.connection.table(self.table_name)
#            for key,data in table.scan():
#                print(key,data)
#            print(table)
            table_exists = True
       #except IOError as e:
        except:
            print("Table does not exists")
            table_exists = False
            create_new = True
        
        if table_exists: 
            print("The table" + self.table_name + "is already exists, do you really want to overwrite it?")
            answer = input("please input yes | no:")
            while(answer != 'yes' and answer != 'no'):
                answer = input("please input yes | no:")
            if answer == 'no':
                print("Going to exit.")
                sys.exit(0)
            else:
                create_new = True 
        if create_new:
            if table_exists:
                self.connection.disable_table(self.table_name)
                self.connection.delete_table(self.table_name)
            print("Start creating table " + self.table_name)
            self.create_table(self.table_name)
        self.table = self.connection.table(self.table_name)

    def create_table(self, table_name):
        families={"cf_properties":{ },
                  "cf_specs":{ }
                  }

        self.connection.create_table(self.table_name, families)
        print("Creating table done ")

        #self.table = self.connection.table(self.table_name)
        #if self.table == Null:
        #else:

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
        
    def import_afile(self):
        """
        import the cluster list  of a file to hbase

        self.id = cluster_id
        self.precursor_mz = precursor_mz
        self.consensus_mz = consensus_mz
        self.consensus_intens = consensus_intens
        
        spectrum:
        self.title = title
        self.precursor_mz = precursor_mz
        self.charge = charge
        self.taxids = frozenset(taxids)
        project_id



        """

        b = self.table.batch()
        for cluster in self.cluster_list:
            spectra = cluster.get_spectra()
            b.put(cluster.id,{
                b'cf_properties:precursor_mz' : str(cluster.precursor_mz),
                b'cf_properties:consensus_mz' : " ".join(str(i) for i in cluster.consensus_mz),
                b'cf_properties:consensus_intens' :  " ".join(str(i) for i in cluster.consensus_intens),
                b'cf_properties:spec_length' : str(len(spectra))
                })
            i =  0
            for spectrum in spectra:
                project_id = self.get_project_id(spectrum.title)

                col_spec_title = "cf_specs:spec_name_" + str(i); 
                col_spec_precursor_mz = "cf_specs:spec_precursor_mz_" + str(i); 
                col_spec_charge = "cf_specs:spec_charge_" + str(i); 
                col_spec_project_id = "cf_specs:spec_prj_id_" + str(i); 
                col_spec_taxids = "cf_specs:spec_taxids_" + str(i); 

                b.put(cluster.id,{
                    col_spec_title.encode() : spectrum.title,
                    col_spec_project_id.encode() : project_id, 
                    col_spec_precursor_mz.encode() : str(spectrum.precursor_mz),
                    col_spec_taxids.encode() : " ".join(str(i) for i in spectrum.taxids),
                    col_spec_charge.encode() : str(spectrum.charge)
                    })
                i += 1

            b.send()

    def clear(self):
        """
        clear the cluster list for a new file
        """
        self.cluster_list = [] 



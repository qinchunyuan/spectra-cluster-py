"""
This analyser import clusters in a .clustering file 
"""

from . import common
import operator 
import happybase
import os,sys
import re
import _thread
import threading
import signal

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
        self.cluster_table_name = "cluster_table_test_17102017"
        self.spec_table_name = "spec_table_test_17102017"
        self.cluster_table = None 
        self.spec_table = None 
       
        # intermediate data structures
        self.cluster_list = [] 

    def connect_and_check(self):
        #build the connection
        self.connection = happybase.Connection(self.hbase_host, autoconnect=False)
        self.connection.open()
        #deal with the table name
#        self.check_table()
        self.cluster_table = self.connection.table(self.cluster_table_name)
        self.spec_table = self.connection.table(self.spec_table_name)
 
    def check_table(self):
        """
        Check the table name exists or not,
        create it if not exists,
        ask user if they want to overwrite the table if exists.
        """
        table_exists = None
        create_new = None
        try:
            table = self.connection.table(self.cluster_table_name)
        except Exception as e:
            print(e)

        try:
            for key,data in table.scan(limit=1):
                print(key, data)
#            print(table)
            if self.connection.is_table_enabled(self.cluster_table_name):
                table_enabled = True
            else : 
                table_enabled = False 
                create_new = True
        except Exception as e:
            print(e)
            print("Table does not exists, or not enabled")
            table_enabled = False
            create_new = True
        
        if table_enabled: 
            print("The table" + self.cluster_table_name + "is already exists, do you really want to overwrite it?")
#            answer = input("please input yes | no:")
            while(answer != 'yes' and answer != 'no'):
                answer = input("please input yes | no:")
          
            print("the answer is" + answer)
            if answer == 'no':
                print("Going to exit.")
                sys.exit(0)
            else:
                create_new = True 
        if create_new:
#            if table_exists:
            try:
                self.connection.disable_table(self.cluster_table_name)
                self.connection.disable_table(self.spec_table_name)
            except Exception as e:
                print(e)
            try:
                self.connection.delete_table(self.cluster_table_name)
                self.connection.delete_table(self.spec_table_name)
            except Exception as e:
                print(e)
    
            print("Start creating table " + self.cluster_table_name)
            try:
                self.create_table(self.cluster_table_name, self.spec_table_name)
            except Exception as e:
                print(e)
                print("Going to exit.")
                sys.exit(0)

    def create_table(self, cluster_table_name, spec_table_name):
        cluster_families={"cf_prop":{ },
                  "cf_specs":{ }
                  }

        spec_families={"cf_prop":{ }
                  }
        try:
            self.connection.create_table(self.cluster_table_name, cluster_families)
            self.connection.create_table(self.spec_table_name, spec_families)
        except Exception as e:
            print(e)

        print("Creating table done ")

        #self.cluster_table = self.connection.table(self.table_name)
        #if self.cluster_table == Null:
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

        cluster_b = self.cluster_table.batch()
        spec_b = self.spec_table.batch()
        for cluster in self.cluster_list:
            spectra = cluster.get_spectra()
            max_sequences = '||'.join(cluster.max_sequences)
            cluster_b.put(cluster.id,{
                b'cf_prop:precursor_mz' : str(cluster.precursor_mz),
                b'cf_prop:consensus_mz' : " ".join(str(i) for i in cluster.consensus_mz),
                b'cf_prop:consensus_intens' :  " ".join(str(i) for i in cluster.consensus_intens),
                b'cf_prop:n_spec' : str(len(spectra)),
                b'cf_prop:n_id_spec' : str(cluster.identified_spectra),
                b'cf_prop:n_unid_spec' : str(cluster.unidentified_spectra),
                b'cf_prop:ratio' : str(cluster.max_il_ratio),
                b'cf_prop:max_freq_seq' : str(max_sequences),
                })
            i =  0
            for spectrum in spectra:
                project_id = self.get_project_id(spectrum.title)

                col_spec_title = "cf_specs:spec_title_" + str(i); 
                col_spec_project_id = "cf_specs:spec_prj_id_" + str(i); 
                cluster_b.put(cluster.id,{
                    col_spec_title.encode() : spectrum.title,
                    col_spec_project_id.encode() : project_id, 
                    })

                col_spec_precursor_mz = "cf_prop:precursor_mz" ; 
                col_spec_charge = "cf_prop:charge" ; 
                col_spec_taxids = "cf_prop:taxids" ; 

                spec_b.put(spectrum.title,{
                    col_spec_precursor_mz.encode() : str(spectrum.precursor_mz),
                    col_spec_charge.encode() : str(spectrum.charge),
                    col_spec_taxids.encode() : " ".join(str(i) for i in spectrum.taxids),
                    })
                
                i += 1

            cluster_b.send()
            spec_b.send()

    def clear(self):
        """
        clear the cluster list for a new file
        """
        self.cluster_list = [] 



"""
This analyser import clusters from a .clustering file 
"""

from . import common
import operator 
import phoenixdb
import os,sys
import re
import traceback


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
        self.type_to_import = ['a'] #import all types(cluster, spectrum, project) by default
        self.file_index = 0
        self.phoenix_host = "http://localhost:8765"
        self.table_name = "t_cluster_20171207".upper()
        self.table = None 
        self.over_write_table = False
        self.check_table_flag = False 
        self.projects = set() 
       
        # intermediate data structures
        self.cluster_list = [] 

    def connect_and_check(self):
        #build the connection
        self.connection = phoenixdb.connect(self.phoenix_host,
                                          autocommit=True)
        print("Opened database successfully");

        #deal with the table name
        if self.check_table_flag:
            self.check_table()
 
    def check_table(self):
        """
        Check the table name exists or not,
        create it if not exists,
        ask user if they want to overwrite the table if exists.
        """
        table_exists = None
        create_new = None
        tb_exists = "SELECT COUNT(*) FROM \"" + self.table_name + "\""
        # create a table
        tb_create = "CREATE TABLE IF NOT EXISTS \"" + self.table_name + "\" ("                     + \
                        "cluster_id VARCHAR(100) NOT NULL PRIMARY KEY,"    + \
                        "cluster_ratio FLOAT ,"    + \
                        "n_spec INTEGER ,"    + \
                        "n_id_spec INTEGER ,"    + \
                        "n_unid_spec INTEGER ,"    + \
                        "sequences_ratios VARCHAR, "    + \
                        "sequences_mods VARCHAR, "    + \
                        "conf_sc VARCHAR, "    + \
                        "spectra_titles VARCHAR, "    + \
                        "consensus_mz VARCHAR, "    + \
                        "consensus_intens VARCHAR "    + \
                        ")"
        tb_create_spec = "CREATE TABLE IF NOT EXISTS \"" + self.table_name + "_spec\" ("                     + \
                        "spec_title VARCHAR(200) NOT NULL PRIMARY KEY, "+ \
                        "spec_prj_id VARCHAR, "   + \
                        "charge FLOAT, "   + \
                        "precursor_mz FLOAT ,"    + \
                        "taxids VARCHAR, "   + \
                        "is_spec_identified INTEGER, "   + \
                        "id_sequences VARCHAR, "   + \
                        "modifications VARCHAR, "   + \
                        "seq_ratio FLOAT, "   + \
                        "cluster_fk VARCHAR(100)"   + \
                        ")"
        tb_create_prjs = "CREATE TABLE IF NOT EXISTS \"" + self.table_name + "_projects\" ("                     + \
                        "project_id VARCHAR(10) PRIMARY KEY"   + \
                        ")"
        with self.connection.cursor() as cursor:
            try:
                print(tb_exists)
                cursor.execute(tb_exists)
                result = cursor.fetchone()
                if result != None :
                    table_exists = True
       #except IOError as e:
                else:
                    print("Table does not exists")
                    table_exists = False
                    create_new = True
            except Exception as e:
                if e.message.startswith("Table undefined. tableName=" + self.table_name):
                    table_exists = False
                    create_new = True
                else:
                    raise e
            try:
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
                        cursor.execute("DROP TABLE IF EXISTS \"" + self.table_name + "_spec\"")
                        cursor.execute("DROP TABLE IF EXISTS \"" + self.table_name + "_projects\"")
                        cursor.execute("DROP TABLE IF EXISTS \"" + self.table_name + "\"")
                    print("Start creating table " + self.table_name)
                    print(tb_create)
                    cursor.execute(tb_create)
                    print(tb_create_prjs)
                    cursor.execute(tb_create_prjs)
                    print(tb_create_spec)
                    cursor.execute(tb_create_spec)
            except Exception as e:
                print(e.message)
            finally:
                print ("checked table")

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

    #todo: do we need to replace I for L? not yet
    def get_seqs_mods(self, spectrum, seq_mods_map):
        sequences = list()
        all_mods = list() 
        for psm in spectrum.psms:
            clean_seq = re.sub(r"[^A-Z]", "", psm.sequence.upper());
            if clean_seq not in sequences: 
                sequences.append(clean_seq)
                ptms = list()
                for ptm in psm.ptms:
                    ptms.append(str(ptm))
                ptm_str = ",".join(ptms)
                all_mods.append(ptm_str)
                if clean_seq not in seq_mods_map.keys():
                    seq_mods_map[clean_seq] = ptm_str
                
        sequences_str = "||".join(sequences)
        all_mods_str = ";".join(all_mods)
        return(sequences_str, all_mods_str, seq_mods_map)


    def import_projects(self):
        if 'a' in self.type_to_import or 'p' in self.type_to_import:
            try:
                with self.connection.cursor() as cursor:
                    for project_id in self.projects:
                        UPSERT_sql = "UPSERT INTO \"" + self.table_name + "_projects\"" \
                                     "(project_id)" + \
                                     "VALUES" + \
                                     "('" + project_id + "')"
                        cursor.execute(UPSERT_sql)
            except Exception as e:
                print(e)



    def import_afile(self):
        """
        import the cluster list  of a file to phoenix

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
        
                        "spectra_titles VARCHAR "    + \
                        "consensus_mz VARCHAR "    + \
                        "consensus_intens VARCHAR "    + \
        
        """
        try:
            with self.connection.cursor() as cursor:
                UPSERT_sql = "UPSERT INTO \"" + self.table_name + "\"" \
                            "(cluster_id, cluster_ratio, n_spec, n_id_spec, n_unid_spec, sequences_ratios, sequences_mods, spectra_titles, consensus_mz, consensus_intens)" + \
                            "VALUES (?,?,?,?,?,?,?,?,?,?)"
                UPSERT_sql2 = "UPSERT INTO \"" + self.table_name + "_spec\"" \
                        "(spec_title, spec_prj_id, charge, precursor_mz, taxids, is_spec_identified, id_sequences, modifications, seq_ratio, cluster_fk)" + \
                        "VALUES (?,?,?,?,?,?,?,?,?,?)"
                cluster_data = []
                spec_data = []
                for cluster in self.cluster_list:
                    spectra = cluster.get_spectra()
                    sequences_ratios = str(cluster.sequence_ratios_il)
                    seq_mods_map = {}
                    n_spec = cluster.n_spectra or 0;
                    n_id = cluster.identified_spectra or 0;
                    n_unid = cluster.unidentified_spectra or 0;
                    spectra_titles = ""
                    consensus_mz = ",".join(map(str,cluster.consensus_mz));
                    consensus_intens = ",".join(map(str,cluster.consensus_intens));
#                    UPSERT_data = "('" + cluster.id + "', " + str(cluster.max_il_ratio) + ", " + str(cluster.n_spectra) + ", " + str(cluster.identified_spectra) + \
#                            ", " + str(cluster.unidentified_spectra) + ", '" + max_sequences + "'),"
                    for spectrum in spectra:
                        if spectra_titles != "":
                            spectra_titles = spectrum.title + "||" + spectra_titles 
                        else:
                            spectra_titles = spectrum.title 
                        project_id = self.get_project_id(spectrum.title)
                        taxids = ",".join(spectrum.taxids)
                        #get the ratio of this spectra/sequence
                        max_seq_ratio = 0
                        for seq in spectrum.get_clean_sequences():
                            seq = seq.replace("I", "L")
                            if cluster.sequence_ratios_il[seq] > max_seq_ratio:
                                max_seq_ratio = cluster.sequence_ratios_il[seq]
                        
                        (sequences, modifications, seq_mods_map) = self.get_seqs_mods(spectrum, seq_mods_map)
                        self.projects.add(project_id)
#                        UPSERT_sql2 +=   "('" + spectrum.title + "', '" + project_id + "', " + str(int(spectrum.is_identified())) + ", '" + cluster.id +"'),"
                        UPSERT_data2 =   (spectrum.title , project_id , spectrum.charge, spectrum.precursor_mz, taxids, spectrum.is_identified(), sequences, modifications, max_seq_ratio, cluster.id )
                        if spectrum.title == None or len(spectrum.title)<1:
                            print("Wrong spectrum title: " + spectrum.title)
                        spec_data.append(UPSERT_data2)
                    seq_mods_str = str(seq_mods_map)
                    UPSERT_data = ( cluster.id, cluster.max_il_ratio, n_spec, n_id, + \
                        n_unid, sequences_ratios, seq_mods_str, spectra_titles, consensus_mz, consensus_intens)
                    cluster_data.append(UPSERT_data)
#                print(cluster_data)
                if 'a' in self.type_to_import or 'c' in self.type_to_import:
                    print("start to import clusters in a file")
                    cursor.executemany(UPSERT_sql, cluster_data)
                if 'a' in self.type_to_import or 's' in self.type_to_import:
                    print("start to import spectra in a file")
                    cursor.executemany(UPSERT_sql2, spec_data)
                print(str(len(cluster_data)) + "clusters have been imported in file.")
        except Exception as ex:
            print(ex)
            traceback.print_exc(file=sys.stdout)
        finally:
            print("UPSERTed a cluster list in to table")

    def clear(self):
        """
        clear the cluster list for a new file
        """
        self.cluster_list = [] 
    def close_db(self):
        self.connection.close()
    


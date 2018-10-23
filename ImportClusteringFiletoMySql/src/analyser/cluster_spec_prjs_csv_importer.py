"""
This analyser import clusters from a .clustering file 
"""

from analyser import common
import pymysql.cursors
import sys
import re
import pandas as pd

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
        self.table_name = "cluster_table_temp2"
        self.test_table_name = "cluster_id_recorder"
        self.table = None 
        self.over_write_table = False
        self.projects = set()

        # intermediate data structures
        self.cluster_list = []

        #write projects info to .csv
        self.projects_id_list = []

        #write_cluster info to .csv
        self.cluster_id_list = []
        self.cluster_max_il_ratio_list = []
        self.cluster_n_spec_list = []
        self.cluster_n_id_list = []
        self.cluster_n_unid_list = []
        self.cluster_sequences_ratios = []
        self.cluster_seq_mods_str = []
        self.cluster_spectra_titles = []
        self.cluster_consensus_mz = []
        self.cluster_consensus_intens = []
        self.cluster_conf_sc_list = []
        self.cluster_columns = ['cluster_id', 'cluster_ratio', 'n_spec', 'n_id', 'n_unid', 'sequences_ratios', 'sequences_mods','spectra_titles', 'consensus_mz', 'consensus_intens','conf_sc']

        #write spectrum info to .csv
        self.spectrum_title_list = []
        self.spectrum_prj_id_list = []
        self.spectrum_charge_list = []
        self.spectrum_precursor_mz = []
        self.spectrum_taxids_list = []
        self.spectrum_is_id_list = []
        self.spectrum_id_seq_list = []
        self.spectrum_modifications_list = []
        self.spectrum_seq_ratio_list = []
        self.spectrum_cluster_fk = []
        self.spectrum_columns = ['spectrum_title', 'charge', 'precursor_mz', 'taxids',
                                 'is_spec_identified', 'id_sequences', 'modifications', 'seq_ratio', 'cluster_fk']

        self.spectrum_dataframe = ""

        self.name = "test"

        self.sub_spectrum_title_list = []
        self.sub_spectrum_charge_list = []
        self.sub_spectrum_precursor_mz = []
        self.sub_spectrum_taxids_list = []
        self.sub_spectrum_is_id_list = []
        self.sub_spectrum_id_seq_list = []
        self.sub_spectrum_modifications_list = []
        self.sub_spectrum_seq_ratio_list = []
        self.sub_spectrum_cluster_fk = []

    def exert_prj_id(self):
        prj_ids = []
        with open('./data/PRIDE_CLUSTER', 'r') as textfile:
            while True:
                lines = textfile.readline()
                if not lines:
                    break
                line = lines.strip("\n")
                prj_ids.append(line)
        textfile.close()
        return prj_ids

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

    # todo: do we need to replace I for L? not yet
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
        return (sequences_str, all_mods_str, seq_mods_map)

    def get_project_id(self, title):
        matchObj = re.match( r'.*?(P[XR]D\d{6}).*', title)
        if matchObj:
#            print("got match" + matchObj.group(1))
            return matchObj.group(1)
        else:
            print("No PRD000000 or PXD000000 be found in the title" + title)
            return None
        
    def import_projects(self):
        for project_id in self.projects:
            self.projects_id_list.append(project_id)
            projects_id_dataframe = pd.DataFrame({"project_id": self.projects_id_list})
            projects_id_dataframe.to_csv("projects_id_02.csv", index=False, sep=',')

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
        for cluster in self.cluster_list:
            spectra = cluster.get_spectra()
            sequences_ratios = str(cluster.sequence_ratios_il)
            seq_mods_map = {}
            n_spec = cluster.n_spectra or 0;
            n_id = cluster.identified_spectra or 0;
            n_unid = cluster.unidentified_spectra or 0;
            spectra_titles = ""
            consensus_mz = ",".join(map(str, cluster.consensus_mz));
            consensus_intens = ",".join(map(str, cluster.consensus_intens));


            for spectrum in spectra:
                if spectra_titles != "":
                    spectra_titles = spectrum.title + "||" + spectra_titles
                else:
                    spectra_titles = spectrum.title
                project_id = self.get_project_id(spectrum.title)
                taxids = ",".join(spectrum.taxids)
                max_seq_ratio = 0
                seq_mods_str = ""
                for seq in spectrum.get_clean_sequences():
                    seq = seq.replace("I", "L")
                    if cluster.sequence_ratios_il[seq] > max_seq_ratio:
                        max_seq_ratio = cluster.sequence_ratios_il[seq]

                (sequences, modifications, seq_mods_map) = self.get_seqs_mods(spectrum, seq_mods_map)
                self.projects.add(project_id)
                seq_mods_str = str(seq_mods_map)

                self.spectrum_title_list.append(spectrum.title)
                self.spectrum_prj_id_list.append(project_id)
                self.spectrum_charge_list.append(spectrum.charge)
                self.spectrum_precursor_mz.append(spectrum.precursor_mz)
                self.spectrum_taxids_list.append(taxids)
                self.spectrum_is_id_list.append(spectrum.is_identified())
                self.spectrum_id_seq_list.append(sequences)
                self.spectrum_modifications_list.append(modifications)
                self.spectrum_seq_ratio_list.append(max_seq_ratio)
                self.spectrum_cluster_fk.append(cluster.id)

            self.cluster_id_list.append(cluster.id)
            self.cluster_max_il_ratio_list.append(cluster.max_il_ratio)
            self.cluster_n_spec_list.append(n_spec)
            self.cluster_n_id_list.append(n_id)
            self.cluster_n_unid_list.append(n_unid)
            self.cluster_sequences_ratios.append(sequences_ratios)
            self.cluster_seq_mods_str.append(seq_mods_str)
            self.cluster_spectra_titles.append(spectra_titles)
            self.cluster_consensus_mz.append(consensus_mz)
            self.cluster_consensus_intens.append(consensus_intens)
            self.cluster_conf_sc_list.append(int(0))

        prj_list = self.exert_prj_id();
        for prj in prj_list:
            for i in range(len(self.spectrum_prj_id_list)):
                if self.spectrum_prj_id_list[i] == prj:
                    self.name = prj
                    self.sub_spectrum_title_list.append(self.spectrum_title_list[i])
                    self.sub_spectrum_charge_list.append(self.spectrum_charge_list[i])
                    self.sub_spectrum_precursor_mz.append(self.spectrum_precursor_mz[i])
                    self.sub_spectrum_taxids_list.append(self.spectrum_taxids_list[i])
                    self.sub_spectrum_is_id_list.append(self.spectrum_is_id_list[i])
                    self.sub_spectrum_id_seq_list.append(self.spectrum_id_seq_list[i])
                    self.sub_spectrum_modifications_list.append(self.spectrum_modifications_list[i])
                    self.sub_spectrum_seq_ratio_list.append(self.spectrum_seq_ratio_list[i])
                    self.sub_spectrum_cluster_fk.append(self.spectrum_cluster_fk[i])
                    #print(self.sub_spectrum_charge_list)
            if len(self.sub_spectrum_title_list):
                spectra_dataframe = pd.DataFrame(
                    {'spectrum_title': self.sub_spectrum_title_list, 'charge': self.sub_spectrum_charge_list,
                     'precursor_mz': self.sub_spectrum_precursor_mz,
                     'taxids': self.sub_spectrum_taxids_list, 'is_spec_identified': self.sub_spectrum_is_id_list,
                     'id_sequences': self.sub_spectrum_id_seq_list,
                     'modifications': self.sub_spectrum_modifications_list,
                     'seq_ratio': self.sub_spectrum_seq_ratio_list, 'cluster_fk': self.sub_spectrum_cluster_fk},
                    columns=self.spectrum_columns)
                spectra_dataframe.to_csv("./results/T_CLUSTER_SPEC_"+self.name, index=False,mode="a",sep=',',header=0)

            self.sub_spectrum_title_list.clear()
            self.sub_spectrum_charge_list.clear()
            self.sub_spectrum_precursor_mz.clear()
            self.sub_spectrum_taxids_list.clear()
            self.sub_spectrum_is_id_list.clear()
            self.sub_spectrum_id_seq_list.clear()
            self.sub_spectrum_modifications_list.clear()
            self.sub_spectrum_seq_ratio_list.clear()
            self.sub_spectrum_cluster_fk.clear()

        #dataSet = spectra_dataframe[['spectrum_title', 'spec_prj_id', 'charge', 'precursor_mz', 'taxids',
#                                 'is_spec_identified', 'id_sequences', 'modifications', 'seq_ratio', 'cluster_fk']]
        #tuples = [tuple(x) for x in dataSet.values]

        cluster_dataframe = pd.DataFrame({'cluster_id':self.cluster_id_list, 'cluster_ratio':self.cluster_max_il_ratio_list, 'n_spec':self.cluster_n_spec_list, 'n_id':self.cluster_n_id_list, 'n_unid':self.cluster_n_unid_list, 'sequences_ratios':self.cluster_sequences_ratios, 'sequences_mods':self.cluster_seq_mods_str,'spectra_titles':self.cluster_spectra_titles, 'consensus_mz':self.cluster_consensus_mz, 'consensus_intens':self.cluster_consensus_intens,'conf_sc':self.cluster_conf_sc_list},columns=self.cluster_columns)
        cluster_dataframe.to_csv("cluster.csv", index=False, mode="a", sep=',',header=0)

    def clear(self):
        """
        clear the all lists for a new file
        """
        self.cluster_list = []

        self.cluster_id_list = []
        self.cluster_max_il_ratio_list = []
        self.cluster_n_spec_list = []
        self.cluster_n_id_list = []
        self.cluster_n_unid_list = []
        self.cluster_sequences_ratios = []
        self.cluster_seq_mods_str = []
        self.cluster_spectra_titles = []
        self.cluster_consensus_mz = []
        self.cluster_consensus_intens = []
        self.cluster_conf_sc_list = []

        self.spectrum_title_list = []
        self.spectrum_prj_id_list = []
        self.spectrum_charge_list = []
        self.spectrum_precursor_mz = []
        self.spectrum_taxids_list = []
        self.spectrum_is_id_list = []
        self.spectrum_id_seq_list = []
        self.spectrum_modifications_list = []
        self.spectrum_seq_ratio_list = []
        self.spectrum_cluster_fk = []

        self.projects_id_list = []

        self.sub_spectrum_title_list = []
        self.sub_spectrum_charge_list = []
        self.sub_spectrum_precursor_mz = []
        self.sub_spectrum_taxids_list = []
        self.sub_spectrum_is_id_list = []
        self.sub_spectrum_id_seq_list = []
        self.sub_spectrum_modifications_list = []
        self.sub_spectrum_seq_ratio_list = []
        self.sub_spectrum_cluster_fk = []




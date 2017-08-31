"""
This analyser convert clusters from .clustering files to an msp file
"""

from . import common
import operator 
import pymysql.cursors
import os,sys
import re


class ClusterConverter(common.AbstractAnalyser):
    """
    This tool converte cluster lists in to an msp file.

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
       
        # intermediate data structures
        self.cluster_list = [] 

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
        if title == None or len(title)<1:
            print("Wrong spectrum title: " + spectrum.title)
        matchObj = re.match( r'.*?(P[XR]D\d{6}).*', title)
        if matchObj:
#            print("got match" + matchObj.group(1))
            return matchObj.group(1)
        else:
            print("No PRD000000 or PXD000000 be found in the title" + title)
            return None
        
    def load_to_file(self, OUT):
        """
        load the cluster list in a file to msp file 

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
            projects = set()
            for spectrum in spectra:
                project_id = self.get_project_id(spectrum.title)
                projects.add(project_id)
            OUT.write("\n")
            OUT.write("Name: _%s/%d\n"%(cluster.id, int(cluster.charge)))
            OUT.write("MW: %f\n"%(cluster.precursor_mz * cluster.charge))
            OUT.write("PrecursorMZ: %f\n"%(cluster.precursor_mz))
            OUT.write("Comment: Size=%d Ratio=%f ProjectN=%d \n"%(cluster.n_spectra, cluster.max_il_ratio, len(projects)))
            OUT.write("NumPeaks: %d\n"%(len(cluster.consensus_mz)))
            for i in range(0, len(cluster.consensus_mz)):
                OUT.write("%f    %f\n"%(cluster.consensus_mz[i], cluster.consensus_intens[i]))

    def clear(self):
        """
        clear the cluster list for a new file
        """
        self.cluster_list = [] 



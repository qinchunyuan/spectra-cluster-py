"""
This analyser import clusters from a .clustering file 
"""

from . import common
import operator 
import os,sys
import re

# make the tools package available
package_path = os.path.abspath(os.path.split(sys.argv[0])[0]) + os.path.sep + ".." + os.path.sep + ".." + os.path.sep + "../mzml_writer"
sys.path.insert(0, package_path)
from mzml_writer import components, binary_encoding, writer
from pyteomics import mzml
import numpy as np
from lxml import etree

class ClusterToMzmlConverter(common.AbstractAnalyser):
    """
    This tool converter cluster to the mzml 
    with same file name or given file name.

    TODO: 
    """
    def __init__(self):
        """
        Initialised a new ClusterToMzmlConverter analyser.

        :return:
        """
#        super().__init__()
        common.AbstractAnalyser.__init__(self)
        self.min_size = 2 # set default minium size 2
        self.file_index = 0
        self.output_name_prefix = ''
       
        # intermediate data structures
        self.cluster_list = [] 

    def process_cluster(self, cluster):
        """
        Add the clusters into cluster list,
        
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
        
    def write_afile(self, output_name):
        """
        write the cluster list  of a file  to mzml 
        """
        output_name_full = self.output_name_prefix + output_name
        """
        self.id = cluster_id
        self.precursor_mz = precursor_mz
        self.consensus_mz = consensus_mz
        self.consensus_intens = consensus_intens
        self.charge = Cluster._calculate_charge(self._spectra)
        """
        charge_array = None

        f = writer.MzMLWriter(open(output_name_full, 'wb'))
        with f:
            f.controlled_vocabularies()
            with f.element('run'):
                with f.element('spectrumList'):
                    for cluster in self.cluster_list:
                        spec_id = cluster.id
                        precursor_mz = cluster.precursor_mz
                        precursor_charge = cluster.charge
                        consensus_mz = cluster.consensus_mz
                        consensus_intens = cluster.consensus_intens
                        precursor_information = {"mz":precursor_mz, "intensity":None, "charge":precursor_charge, "scan_id":None} #mz, intensity, charge, scan_id
                        f.write_spectrum(consensus_mz, consensus_intens, charge_array, id=spec_id, params=[
                            {"name": "ms level", "value": 2}, {"name": "ClusterUniID", "value": spec_id}], centroided=True, precursor_information=precursor_information)


    def clear(self):
        """
        clear the cluster list for a new file
        """
        self.cluster_list = [] 



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
        self.projects = set() 
       
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
        matchObj = re.match( r'.*?(P[XR]D\d{6}).*', title)
        if matchObj:
#            print("got match" + matchObj.group(1))
            return matchObj.group(1)
        else:
            print("No PRD000000 or PXD000000 be found in the title" + title)
            return None
        
    def import_projects(self, output_path):
        with open(output_path + '/projects_in_clusters.tab', 'w') as o:
            for project_id in self.projects:
                line ="%s\n"%(project_id)
                o.write(line)        
        o.close() 
    
    
    
    def import_afile(self, output_file):
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
            with open (output_file, 'w') as o:
                line = "%s\t%s\t%s\t%s\t%s\t%s\t%s\n"% \
                            ('cluster_id', 'max_ratio', 'n_spectra', 'n_identified', \
                            'n_unidentified','max_sequences','projects')
                for cluster in self.cluster_list:
                    spectra = cluster.get_spectra()
                    projects_in_cluster = set()
                    for spectrum in spectra:
                        project_id = self.get_project_id(spectrum.title)
                        self.projects.add(project_id)
                        projects_in_cluster.add(project_id)

                    max_sequences = '||'.join(cluster.max_sequences)
                    projects = '||'.join(projects_in_cluster)

                    line = "%s\t%s\t%s\t%s\t%s\t%s\t%s\n"% \
                            (cluster.id, str(cluster.max_il_ratio), str(cluster.n_spectra), str(cluster.identified_spectra), \
                            str(cluster.unidentified_spectra), max_sequences, projects)
                    o.write(line)

                    """
                        insert_sql2 = "INSERT INTO `" + self.table_name + "_spec`" \
                            "(spectrum_title, spec_prj_id, is_spec_identified, cluster_fk)" + \
                            "VALUES" + \
                            "('" + spectrum.title + "', '" + project_id + "', '" + str(int(spectrum.is_identified())) + "', '" + str(cluster_key) +"');"
                        if spectrum.title == None or len(spectrum.title)<1:
                            print("Wrong spectrum title: " + spectrum.title)
                        cursor.execute(insert_sql2)        
                    """
#            self.connection.commit()
        except Exception as ex:
            print(ex)
            sys.exit(1)
        finally:
            print("inserted a cluster list in to tab file %s"%(output_file))

    def clear(self):
        """
        clear the cluster list for a new file
        """
        self.cluster_list = [] 


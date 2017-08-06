"""
cluster_to_mzml

Command line interface to the spectra-cluster converter to mzml. This tool convert all listed  
clustering files to mzml files with the same file names. 

Usage:
  cluster_to_mzml.py --input <path_to_clustering_files>
                       [--min_size <size>] 
                       [--min_ratio <ratio>]
                       [--min_identified <spectra>]
                       [--output_name_prefix <output_name_prefix>] 
                       [(--only_identified | --only_unidentified)]
  cluster_to_mzml.py (--help | --version)

Options:
  -i, --input=<path_to_clustering_files>   Path to the directory with .clustering result files to process.
  --min_size=<size>                    The minimum size of a cluster to be reported. [default: 2]
  --min_ratio=<ratio>                  The minimum ratio a cluster must have to be reported.
  --min_identified=<spectra>           May specify the minimum number of identified spectra a cluster must have.
  --output_name_prefix=<output_name_prefix>           The prefix name of the output mzXML file 
  --only_identified                    If set, only identified spectra will be reported.
  --only_unidentified                  If set, only unidentified spectra will be reported.
  -h, --help                           Print this help message.
  -v, --version                        Print the current version.
"""

import sys
import os
import glob
from docopt import docopt
    
# make the spectra_cluster packages available
package_path = os.path.abspath(os.path.split(sys.argv[0])[0]) + os.path.sep + ".." + os.path.sep + ".."
sys.path.insert(0, package_path)

# make the tools package available
package_path = os.path.abspath(os.path.split(sys.argv[0])[0]) + os.path.sep + ".." + os.path.sep + ".." + os.path.sep + ".."
sys.path.insert(0, package_path)

import spectra_cluster.analyser.cluster_to_mzml_converter as converter
import spectra_cluster.clustering_parser as clustering_parser
#from mzml_writer.mzml_writer import *

def create_analyser(arguments):
    """
    Creates an convertor analyser based on the command line
    parameters.
    :param arguments: The command line parameters
    :return: An Comparer object
    """
    analyser = converter.ClusterToMzmlConverter()

    if arguments["--only_identified"]:
        analyser.add_to_unidentified = False

    if arguments["--only_unidentified"]:
        analyser.add_to_identified = False

    if arguments["--min_size"]:
        analyser.min_size = int(arguments["--min_size"])

    if arguments["--min_ratio"]:
        analyser.min_ratio = float(arguments["--min_ratio"])
    
    if arguments["--output_name_prefix"]:
        analyser.output_name_prefix = arguments["--output_name_prefix"]

    if arguments["--min_identified"] is not None:
        analyser.min_identified_spectra = int(arguments["--min_identified"])

    return analyser

def main():
    """
    Primary entry function for the CLI.
    :return:
    """
    arguments = docopt(__doc__, version='cluster_to_mzml 1.0 BETA')
    print(arguments)

    # make sure the input path exists and has .clustering files
    input_path = arguments['--input']
    clustering_files = glob.glob(input_path + "/*.clustering")
    if len(clustering_files) < 1:
        print("Error: Cannot find .clustering in path '" + input_path+ "'")
        sys.exit(1)
        
    for clustering_file in clustering_files:
        if not os.path.isfile(clustering_file):      
            print("Error: this clustering file is not a file '" + clustering_file + "'")
            sys.exit(1)

    # create the cluster comparer based on the settings
    analyser = create_analyser(arguments)
#    analyser.connect_and_check()

    print("Parsing input .clustering files...")
    # process all clustering files
    for clustering_file in clustering_files:
        output_file = clustering_file[:-10] + "mzML"
        parser0 = clustering_parser.ClusteringParser(clustering_file)
        for cluster in parser0:
            analyser.process_cluster(cluster)
    	# do the  importing to database 
        analyser.write_afile(output_file) 
        analyser.clear() 
        print("Done converting of " + clustering_file)



if __name__ == "__main__":
    main()

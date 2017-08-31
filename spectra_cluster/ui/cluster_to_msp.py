"""
cluster_to_msp

Command line interface to the spectra-cluster msp converter. This tool merge all listed  
clustering files into a msp file, for preparing a spectra library. 

Usage:
  cluster_to_msp.py --input <path_to_clustering_files>
                       [--input_files <input_files>]
                       [--output <msp_file>]
                       [--min_size <size>] 
                       [--min_ratio <ratio>]
                       [--min_identified <spectra>]
                       [(--only_identified | --only_unidentified)]
  cluster_to_msp.py (--help | --version)

Options:
  -i, --input=<path_to_clustering_files>   Path to the directory with .clustering result files to process.
  --input_files=<clustering_files>     Path of the .clustering result files to process.
  -o, --output=<msp_file>                  Path to msp result files to load.
  --min_size=<size>                    The minimum size of a cluster to be reported. [default: 2]
  --min_ratio=<ratio>                  The minimum ratio a cluster must have to be reported.
  --min_identified=<spectra>           May specify the minimum number of identified spectra a cluster must have.
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

import spectra_cluster.analyser.cluster_to_msp_analyser as converter
import spectra_cluster.clustering_parser as clustering_parser


def create_analyser(arguments):
    """
    Creates an comparer analyser based on the command line
    parameters.
    :param arguments: The command line parameters
    :return: An Comparer object
    """
    analyser = converter.ClusterConverter()


    if arguments["--only_identified"]:
        analyser.add_to_unidentified = False

    if arguments["--only_unidentified"]:
        analyser.add_to_identified = False

    if arguments["--min_size"]:
        analyser.min_size = int(arguments["--min_size"])

    if arguments["--min_ratio"]:
        analyser.min_ratio = float(arguments["--min_ratio"])
    
    if arguments["--min_identified"] is not None:
        analyser.min_identified_spectra = int(arguments["--min_identified"])

    return analyser

def main():
    """
    Primary entry function for the CLI.
    :return:
    """
    arguments = docopt(__doc__, version='cluster_to_msp 1.0 BETA')
#    print(arguments)

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


    if arguments['--input_files']:
        input_files = arguments['--input_files']
        clustering_files = input_files.split(' ')

    if arguments["--output"] :
        output_file = arguments["--output"] 

    if os.path.exists(output_file):      
        raise Exception("Output file %s is already there!"%(output_file))
    OUT = open(output_file, 'w')
    # create the cluster converter based on the settings
    converter = create_analyser(arguments)

    print("Parsing input .clustering files...")
    # process all clustering files
    for clustering_file in clustering_files:
        parser0 = clustering_parser.ClusteringParser(clustering_file)
        for cluster in parser0:
            converter.process_cluster(cluster)
    	# do the  importing to database 
        converter.load_to_file(OUT) 
        converter.clear() 
        print("Done converting of " + clustering_file)
#    analyser.import_projects()
    
    OUT.close()


if __name__ == "__main__":
    main()

"""
batch_cluster_to_tab_converter

Command line interface to the spectra-cluster phoenix importer. This tool import all listed  
clustering files into phoenix as a sigle release table. 

Usage:
  batch_cluster_to_tab_converter.py --input <path_to_clustering_files>
                       [--min_size <size>] 
                       [--min_ratio <ratio>]
                       [--min_identified <spectra>]
                       [--table_name <table_name>] 
                       [--output_path <output_path>] 
                       [--host <host_name>] 
                       [(--over_write_table)] 
                       [(--only_identified | --only_unidentified)]
  batch_cluster_to_tab_converter.py (--help | --version)

Options:
  -i, --input=<path_to_clustering_files>   Path to the directory with .clustering result files to process.
  --min_size=<size>                    The minimum size of a cluster to be reported. [default: 2]
  --min_ratio=<ratio>                  The minimum ratio a cluster must have to be reported.
  --min_identified=<spectra>           May specify the minimum number of identified spectra a cluster must have.
  --table_name=<table_name>            The table to store this cluster release 
  --output_path=<output_path>          The path to tab file to store this cluster release 
  --host=<host_name>                   The host phoenix  to store this cluster release 
  --over_write_table                   If set, the table will be over write directly.
  --only_identified                    If set, only identified spectra will be reported.
  --only_unidentified                  If set, only unidentified spectra will be reported.
  -h, --help                           Print this help message.
  -v, --version                        Print the current version.
"""

import sys
import os
import glob
from docopt import docopt
   
import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


def filter(filename):
    #    logger.info()
    arguments = docopt(__doc__, version='batch_cluster_to_tab_converter 1.0 BETA')
    print(arguments)
    p = re.compile(".*\/(.*?.clustering)")
    m = p.match(filename)
    relativeName = ""
    if m:
        relativeName = m.group(1)
        print(relativeName)
    else:
        print("ERROR, can not get relative name")
    commandLine = "python3 /home/ubuntu/mingze/tools/spectra-cluster-py/spectra_cluster/ui/cluster_phoenix_importer.py " + \
        " --input " +  filename + \
        " --min_size " + arguments['--min_size']
    print("Executing %s"%(commandLine))    
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    print("Return value %s"%(retval))
    if retval != 0:
        print("ERROR" + str(p.stdout.read()))
    else : 
        print("Done importing file" + filename)
                                                                                                                                          
                                                                                                                                          


def main():
    """
    Primary entry function for the CLI.
    :return:
    """
    arguments = docopt(__doc__, version='batch_cluster_to_tab_converter 1.0 BETA')


    input_path = arguments['--input']
    clustering_files = glob.glob(input_path + "/*.clustering")

    if len(clustering_files) < 1:
        print("Error: Cannot find .clustering in path '" + input_path+ "'")
        sys.exit(1)
    files_abs_path = list()    
    for clustering_file in clustering_files:
        if not os.path.isfile(clustering_file):      
            print("Error: this clustering file is not a file '" + clustering_file + "'")
            sys.exit(1)
        files_abs_path.append(os.path.abspath(clustering_file)) 
    print("Start to do the parallel batch importing!")
    pool = ThreadPool(26)
    pool.map(filter, files_abs_path)
    pool.close()



if __name__ == "__main__":
    main()

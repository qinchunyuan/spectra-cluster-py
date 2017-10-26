"""
cluster_hbase_importer

Command line interface to the spectra-cluster hbase importer. This tool import all listed  
clustering files into hbase as a sigle release table. 

Usage:
  cluster_hbase_importer.py --input <path_to_clustering_files>
                       [--min_size <size>] 
                       [--min_ratio <ratio>]
                       [--min_identified <spectra>]
                       [--host <hostname or ip>]
                       [--table_name <table_name>] 
                       [(--only_identified | --only_unidentified)]
  cluster_hbase_importer.py (--help | --version)

Options:
  -i, --input=<path_to_clustering_files>   Path to the directory with .clustering result files to process.
  --min_size=<size>                    The minimum size of a cluster to be reported. [default: 2]
  --min_ratio=<ratio>                  The minimum ratio a cluster must have to be reported.
  --min_identified=<spectra>           May specify the minimum number of identified spectra a cluster must have.
  --table_name=<table_name>            The table to store this cluster release 
  --host=<hostname_or_ip>              The host name or ip of the HBase server 
  --only_identified                    If set, only identified spectra will be reported.
  --only_unidentified                  If set, only unidentified spectra will be reported.
  -h, --help                           Print this help message.
  -v, --version                        Print the current version.
"""

from docopt import docopt
import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

# make the spectra_cluster packages available
package_path = os.path.abspath(os.path.split(sys.argv[0])[0]) + os.path.sep + ".." + os.path.sep + ".."
sys.path.insert(0, package_path)

import spectra_cluster.analyser.cluster_hbase_importer as cluster_hbase_importer
import spectra_cluster.clustering_parser as clustering_parser
import cluster_hbase_importer as importer


def filter(filename):
    arguments = docopt(__doc__, version='batch_cluster_to_hbase_converter 1.0 BETA')
    print(arguments)
    p = re.compile(".*\/(.*?.clustering)")
    m = p.match(filename)
    relativeName = ""
    if m:
        relativeName = m.group(1)
        print(relativeName)
    else:
        print("ERROR, can not get relative name")
    commandLine = "python3 /home/ubuntu/mingze/tools/spectra-cluster-py/spectra_cluster/ui/cluster_hbase_importer.py " + \
        " --input " +  filename + \
        " --min_size " + arguments['--min_size']
    print("Executing %s"%(commandLine))    
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    if retval != 0:
        print("Return value %s"%(retval))
        print("ERROR" + str(p.stdout.read()))
    else: 
        print("Done importing of " + filename) 

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
    pool = ThreadPool(22)
    pool.map(filter, files_abs_path)
    pool.close()

if __name__ == "__main__":
    main()

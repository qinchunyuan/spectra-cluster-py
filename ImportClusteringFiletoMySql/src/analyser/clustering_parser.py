# ------------------------------
# This is a first implementation of
# a parser class to process
# .clustering output files
# -------------------------------
from analyser import objects

class ClusteringParser:
    """Parses .clustering output files created by the spectra-cluster applications.
    """

    def __init__(self, clustering_file):
        """
        Processes the passed .clustering file

        :param clustering_file: Path to the file to process
        :return:
        """
        self.clustering_file = clustering_file

    def __iter__(self):
        return self._get_iterator()

    def _get_iterator(self):
        """
        Iterates over all clusters in a file

        :return: The next Cluster
        """
        spectra = list()
        cur_id = None
        precursor_mz = None
        consensus_mz = list()
        consensus_intens = list()

        with open(self.clustering_file, "r") as clustering_input:
            for line in clustering_input:
                line = line.strip()
                if line == "=Cluster=":
                    # create and return the cluster
                    if cur_id is not None:
                        cluster = objects.Cluster(cur_id, precursor_mz, consensus_mz, consensus_intens, spectra)
                        yield cluster

                    # reset parameters
                    cur_id = None
                    precursor_mz = None
                    consensus_mz = list()
                    consensus_intens = list()
                    spectra = list()

                    continue

                # process the spectrum
                if line[0:4] == "SPEC":
                    spectra.append(ClusteringParser._parse_spec_line(line))
                    continue

                # process standard fields
                if line[0:3] == "id=":
                    cur_id = line[3:]
                if line[0:16] == "av_precursor_mz=":
                    precursor_mz = float(line[16:])
                if line.lstrip()[0:13] == "consensus_mz=":
                    # this is only a work around for empty consensus spectrum entries
                    if len(line) < 14:
                        consensus_mz = list()
                    else:
                        consensus_mz = [float(s) for s in line[13:].split(",")]
                if line[0:17] == "consensus_intens=":
                    if len(line) < 18:
                        consensus_intens = list()
                    else:
                        consensus_intens = [float(s) for s in line[17:].split(",")]

        # process the last cluster
        if cur_id is not None:
            cluster = objects.Cluster(cur_id, precursor_mz, consensus_mz, consensus_intens, spectra)
            yield cluster

    @staticmethod
    def _parse_spec_line(line):
        """
        Parses a .clustering SPEC line and creates the corresponding PSM object

        :param line: A String representing the SPEC line.
        :return: A PSM object.
        """
        fields = line.split("\t")

        if len(fields) < 9:
            raise Exception("Invalid SPEC line encountered: " + line)

        title = fields[1]
        sequences = fields[3].split(",")
        prec_mz = float(fields[4])
        # charge is stored as float just in case
        charge = float(fields[5])
        taxids = fields[6].split(",")
        ptm_strings = fields[7].split(";")
        similarity_score = float(fields[8])

        json_properties = fields[9] if len(fields) >= 10 else "{}"

        if title == "PXD000443;PRIDE_Exp_Complete_Ac_31019.xml;spectrum=377050":
            pass

        # make sure sequences and ptms have the same length
        if len(sequences) != len(ptm_strings):
            raise Exception("Invalid SPEC line encountered: different number of sequences and PTMs defined: " +
                            line)

        psms = ClusteringParser._create_psms(sequences, ptm_strings)

        return objects.Spectrum(title, prec_mz, charge, taxids, psms, similarity_score, json_properties)

    @staticmethod
    def _create_psms(sequences, ptm_strings):
        """
        Creates a list of PSM objects based on the information in the
        clustering's file SPEC line.

        :param sequences: List of peptide sequences observed
        :param ptm_strings: List of PTM string definitions
        :return: List of PSM objects
        """
        if len(sequences) != len(ptm_strings):
            raise Exception("Different number of peptide sequences and PTM specifications "
                            "observed in SPEC line.")

        # test if it's an empty line
        if len(sequences) == 1 and len(sequences[0].strip()) == 0:
            return list()

        psms = list()

        for i in range(0, len(sequences)):
            cur_ptms = ClusteringParser._parse_ptms(ptm_strings[i])
            psms.append(objects.PSM(sequences[i], cur_ptms))

        # TODO: make sure the PSMs are unique

        return psms

    @staticmethod
    def _parse_ptms(ptm_string):
        """
        Creates a list of PTMs based on a PTM specification string.

        :param ptm_string: String defining the PTMs in a .clustering SPEC line.
        :return: List of PTMs
        """
        if len(ptm_string) < 1:
            return list()

        ptm_strings = ptm_string.split(",")

        # merge possible PTM tags within "[" and "]"
        merged_ptm_strings = list()
        current_ptm_string = ""
        in_tags = False
        for ptm_string in ptm_strings:
            if "[" in ptm_string:
                in_tags = True
                current_ptm_string = ptm_string.strip() + ","
                continue
            # this is a standard mod
            elif not in_tags:
                merged_ptm_strings.append(ptm_string)
                continue
            # process fields that are within tags
            if in_tags:
                current_ptm_string += ptm_string.strip() + ","
            # if at the end of the tag, save it and reset it
            if in_tags and "]" in ptm_string:
                in_tags = False
                # save the PTM string without the trailing ","
                merged_ptm_strings.append(current_ptm_string[:-1])
                current_ptm_string = ""

        # remove unused variable
        del ptm_string, current_ptm_string, in_tags

        ptms = list()

        for cur_ptm_string in merged_ptm_strings:
            first_index = cur_ptm_string.find("-")

            if first_index < 0:
                print("Warning: Ignoring invalid PTM definition: " + cur_ptm_string)
                continue

            position = cur_ptm_string[0:first_index]
            accession = cur_ptm_string[first_index + 1:]

            # ignore chemmod
            if "CHEMMOD" in position:
                print("Warning: Ignoring PTM (" + cur_ptm_string + ")")
                continue

            ptms.append(objects.PTM(int(position), accession))

        return ptms



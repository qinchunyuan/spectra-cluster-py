import unittest
import os
import sys
sys.path.insert(0, os.path.abspath('..'))
import spectra_cluster.clustering_parser as clustering_parser
import spectra_cluster.analyser.id_transferer as id_transferer


class IdTransfererTest(unittest.TestCase):
    """
    Test case for the IdTransfererTest class
    """
    def setUp(self):
        self.testfile = os.path.join(os.path.dirname(__file__), "test.clustering")

    def test_identifiy_all(self):
        parser = clustering_parser.ClusteringParser(self.testfile)
        analyser = id_transferer.IdTransferer(True, True)

        for cluster in parser:
            analyser.process_cluster(cluster)

        self.assertEqual(3149, len(analyser.identification_references))
        self.assertEqual(1, len(analyser.identification_references[11].psms))
        self.assertEqual("HQGVMVGMGQK", analyser.identification_references[11].psms[0].sequence)
        self.assertEqual("/home/jg/Projects/ebi-pride/pride-cluster-2/chimeric-spectra-generator/src/test/resources/PRD000001.st.id.mgf",
                         analyser.identification_references[11].filename)

        ref4 = analyser.identification_references[4]
        self.assertEqual(1, len(ref4.psms))
        self.assertEqual("MEGIGLK", ref4.psms[0].sequence)

    def test_identify_unidentified(self):
        parser = clustering_parser.ClusteringParser(self.testfile)
        analyser = id_transferer.IdTransferer(add_to_identified=False, add_to_unidentified=True)

        for cluster in parser:
            analyser.process_cluster(cluster)

        self.assertEqual(0, len(analyser.identification_references))

    def test_identify_identified(self):
        parser = clustering_parser.ClusteringParser(self.testfile)
        analyser = id_transferer.IdTransferer(add_to_identified=True, add_to_unidentified=False)

        for cluster in parser:
            analyser.process_cluster(cluster)

        self.assertEqual(3149, len(analyser.identification_references))

    def test_keep_identified(self):
        parser = clustering_parser.ClusteringParser(self.testfile)
        analyser = id_transferer.IdTransferer(False, True, True)

        for cluster in parser:
            analyser.process_cluster(cluster)

        self.assertEqual(3149, len(analyser.identification_references))
        self.assertEqual(1, len(analyser.identification_references[11].psms))
        self.assertEqual("HQGVMVGMGQK", analyser.identification_references[11].psms[0].sequence)
        self.assertEqual("/home/jg/Projects/ebi-pride/pride-cluster-2/chimeric-spectra-generator/src/test/resources/PRD000001.st.id.mgf",
                         analyser.identification_references[11].filename)

        ref4 = analyser.identification_references[4]
        self.assertEqual(1, len(ref4.psms))
        sequence_counts = dict()

        for id_ref in analyser.identification_references[2:5]:
            for psm in id_ref.psms:
                if psm.sequence not in sequence_counts:
                    sequence_counts[psm.sequence] = 1
                else:
                    sequence_counts[psm.sequence] += 1

        self.assertEqual(2, sequence_counts["MEGIGLK"])
        self.assertEqual(1, sequence_counts["ASDA"])

if __name__ == "__main__":
    unittest.main()

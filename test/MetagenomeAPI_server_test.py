# -*- coding: utf-8 -*-
import unittest
import os
import json  # noqa: F401
import time
import requests  # noqa: F401
import shutil

from os import environ
try:
    from ConfigParser import ConfigParser  # py2
except:
    from configparser import ConfigParser  # py3

# from pprint import pprint
import pprint
pp = pprint.PrettyPrinter(indent=4)
_DIR = os.path.dirname(os.path.realpath(__file__))

from MetagenomeUtils.MetagenomeUtilsClient import MetagenomeUtils
from AssemblyUtil.AssemblyUtilClient import AssemblyUtil

from biokbase.workspace.client import Workspace as workspaceService
from MetagenomeAPI.MetagenomeAPIImpl import MetagenomeAPI
from MetagenomeAPI.MetagenomeAPIServer import MethodContext
from MetagenomeAPI.authclient import KBaseAuth as _KBaseAuth
from installed_clients.DataFileUtilClient import DataFileUtil


class MetagenomeAPITest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        token = environ.get('KB_AUTH_TOKEN', None)
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('MetagenomeAPI'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)
        cls.ctx.update({'token': token,
                        'user_id': user_id,
                        'provenance': [
                            {'service': 'MetagenomeAPI',
                             'method': 'please_never_use_it_in_production',
                             'method_params': []
                             }],
                        'authenticated': 1})
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = workspaceService(cls.wsURL)
        cls.serviceImpl = MetagenomeAPI(cls.cfg)
        cls.scratch = cls.cfg['scratch']
        cls.callback_url = os.environ['SDK_CALLBACK_URL']

        suffix = int(time.time() * 1000)
        cls.wsName = "test_kb_maxbin_" + str(suffix)
        cls.ws_info = cls.wsClient.create_workspace({'workspace': cls.wsName})

        # you could bypass creating objects for more rapid testing here
        # cls.binnedcontigs_ref_1 = '19621/2/1'
        # cls.assembly_ref_1 = '19621/1/1'
        # return
        cls.name_ref = "KBaseTestData/metagenome_badabing.assembly.fa_metagenome/1"
        # create some test data
        cls.au = AssemblyUtil(cls.callback_url)
        cls.mu = MetagenomeUtils(cls.callback_url)
        cls.dfu = DataFileUtil(cls.callback_url)
        # name needs to be converted to id, or will fail tests
        cls.ref = cls.wsClient.get_objects2({'objects': [{'ref': cls.name_ref}]})['data'][0]['path'][0]
        # building Assembly
        assembly_filename = 'small_bin_contig_file.fasta'
        cls.assembly_fasta_file_path = os.path.join(cls.scratch, assembly_filename)
        shutil.copy(os.path.join("data", assembly_filename), cls.assembly_fasta_file_path)

        assembly_params = {
            'file': {'path': cls.assembly_fasta_file_path},
            'workspace_name': cls.wsName,
            'assembly_name': 'MyAssembly'
        }
        print(os.path.isfile(cls.assembly_fasta_file_path))

        cls.assembly_ref_1 = cls.au.save_assembly_from_fasta(assembly_params)
        print('Assembly1:' + cls.assembly_ref_1)

        # stage and build BinnedContigs data
        test_directory_name = 'test_maxbindata'
        cls.test_directory_path = os.path.join(cls.scratch, test_directory_name)
        os.makedirs(cls.test_directory_path)
        print(os.listdir(cls.test_directory_path))
        for item in os.listdir(os.path.join("data", "MaxBin_Result_Sample")):
            shutil.copy(os.path.join("data", "MaxBin_Result_Sample", item),
                        os.path.join(cls.test_directory_path, item))

        cls.binnedcontigs_ref_1 = cls.mu.file_to_binned_contigs({'file_directory': cls.test_directory_path,
                                                                 'assembly_ref': cls.assembly_ref_1,
                                                                 'binned_contig_name': 'MyBins',
                                                                 'workspace_name': cls.wsName
                                                                 })['binned_contig_obj_ref']
        print('BinnedContigs1:' + cls.binnedcontigs_ref_1)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'ws_info'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')
        pass

    def getWsClient(self):
        return self.__class__.wsClient

    def getWsName(self):
        return self.ws_info[1]

    def getWsID(self):
        return self.ws_info[0]

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    def check_ret(self, ret, incl):
        self.assertTrue('genomes' in ret)
        data = ret.get('genomes')
        data = data[0]['data']
        for field in incl:
            self.assertTrue(field in data, msg=f"{field} not in returned output. Available fields {data.keys()}")
        # make sure one of the standard fields is not included
        self.assertFalse('features_handle_ref' in data)

    def save_metagenome(self):
        json_file = "data/test_metagenome_object.json"
        with open(json_file) as f:
            data = json.load(f)

        data['assembly_ref'] = "KBaseTestData/metagenome_test_annotated.assembly.fa_assembly/1"
        data['ontology_events'][0]['ontology_ref'] = "KBaseOntology/gene_ontology/1"
        # create a new fake handle to solve the handle permission issue in original json data file
        # these handles are not used in tests
        shutil.copy("data/features_handle_ref", os.path.join(self.scratch, 'features_handle_ref'))
        uploaded = self.dfu.file_to_shock(
            {'file_path': os.path.join(self.scratch, 'features_handle_ref'),
             'make_handle': 1})
        fhandle = uploaded['handle']['hid']
        data["features_handle_ref"] = fhandle
        data["gff_handle_ref"] = fhandle
        data["protein_handle_ref"] = fhandle

        obj_info = self.dfu.save_objects({
            'id': self.getWsID(),
            "objects": [{
                'type': "KBaseMetagenomes.AnnotatedMetagenomeAssembly",
                'data': data,
                'name': "test_metagenome"
            }]
        })[0]
        return '/'.join([str(obj_info[6]), str(obj_info[0]), str(obj_info[4])])

    # @unittest.skip('x')
    def test_search_feature_counts_by_type(self):
        """
        """
        self.maxDiff = None
        params = {
            "ref": self.ref
        }
        ret = self.getImpl().get_feature_type_counts(self.getContext(), params)[0]
        check = {'feature_type_counts': {'CDS': 131854, 'gene': 131854, 'tRNA': 1024, 'repeat_region': 422, 'rRNA': 287}}
        self.assertEquals(ret, check)

    # @unittest.skip('x')
    def test_get_contig_info(self):
        """test the 'get_contig_info' function
        NOTE: This test is tied to a version of workspace object in elasticsearch.
        """
        params = {
            "ref": self.ref,
            "contig_id": "Ga0065724_100164"
        }
        ret = self.getImpl().get_contig_info(self.getContext(), params)[0]
        self.assertTrue('contig' in ret)
        self.assertTrue('length' in ret['contig'])
        self.assertTrue('contig_id' in ret['contig'])
        self.assertTrue('feature_count' in ret['contig'])
        check = {'contig': {'contig_id': "Ga0065724_100164", 'length': 28310, 'feature_count': 60}}
        self.assertEquals(ret, check)

    # @unittest.skip('x')
    def test_search_contigs(self):
        """test the 'search_contigs' function
        NOTE: This test is tied to a version of workspace object in elasticsearch.
        """
        self.maxDiff=None
        # sort by 'length'
        params = {
            "ref": self.ref,
            "start": 0,
            "limit": 10,
            "sort_by": ("length", 0)
        }
        b = time.time()
        ret = self.getImpl().search_contigs(self.getContext(), params)[0]
        # print(f"\n------'length' sort takes {time.time() - b} seconds to run------\n")
        self.assertTrue('contigs' in ret)
        self.assertTrue('start' in ret)
        self.assertTrue('num_found' in ret)
        self.assertEquals(len(ret['contigs']), 10)
        self.assertTrue(sum([c['feature_count'] for c in ret['contigs']]) > 0)
        self.assertEquals([c['length'] for c in ret['contigs']],
                          sorted([c['length'] for c in ret['contigs']], reverse=True))
        # sort by 'contig_id'
        params = {
            "ref": self.ref,
            "start": 0,
            "limit": 10,
            "sort_by": ("contig_id", 1)
        }
        b = time.time()
        ret = self.getImpl().search_contigs(self.getContext(), params)[0]
        # print(f"\n------'contig_id' sort takes {time.time() - b} seconds to run------\n")
        self.assertTrue('contigs' in ret)
        self.assertTrue('start' in ret)
        self.assertTrue('num_found' in ret)
        self.assertEquals(len(ret['contigs']), 10)
        self.assertTrue(sum([c['feature_count'] for c in ret['contigs']]) > 0)
        self.assertEquals([c['contig_id'] for c in ret['contigs']],
                          sorted([c['contig_id'] for c in ret['contigs']]))
        # sort by 'feature_count'
        params = {
            "ref": self.ref,
            "start": 0,
            "limit": 10,
            "sort_by": ("feature_count", 0)
        }
        b = time.time()
        ret = self.getImpl().search_contigs(self.getContext(), params)[0]
        # print(f"\n------'feature_count' sort takes {time.time() - b} seconds to run------\n")
        self.assertTrue('contigs' in ret)
        self.assertTrue('start' in ret)
        self.assertTrue('num_found' in ret)
        self.assertEquals(len(ret['contigs']), 10)
        self.assertTrue(sum([c['feature_count'] for c in ret['contigs']]) > 0)
        self.assertEquals([c['feature_count'] for c in ret['contigs']],
                          sorted([c['feature_count'] for c in ret['contigs']], reverse=True))

    # @unittest.skip('x')
    def test_region_search(self):
        """test the 'search_region' function
        NOTE: This test is tied to a version of workspace object in elasticsearch.
        """
        # self.maxDiff=None
        params = {
            "ref": self.ref,
            "contig_id": "Ga0065724_100164",
            "region_start": 20000,
            "region_length": 20000,
            "page_start": 0,
            "page_limit": 100,
            "sort_by": [("starts", 1), ('stops', 1)]
        }
        ret = self.getImpl().search_region(self.getContext(), params)[0]
        self.assertTrue('contig_id' in ret, msg=f"returned: {ret.keys()}")
        self.assertTrue('region_length' in ret, msg=f"returned: {ret.keys()}")
        self.assertTrue('features' in ret, msg=f"returned: {ret.keys()}")
        self.assertTrue('region_start' in ret, msg=f"returned: {ret.keys()}")
        self.assertTrue('num_found' in ret, msg=f"returned: {ret.keys()}")
        self.assertTrue('start' in ret, msg=f"returned: {ret.keys()}")
        # compare_path = os.path.join(_DIR, "data", "search_region_test_resp_ci_43655_58_1.json")
        # with open(compare_path) as f:
        #     compare = json.load(f)
        # self.assertEqual(ret, compare)

    # @unittest.skip('x')
    def test_search_query(self):
        """test the 'search' function, using a query string.
        NOTE: This test is tied to a version of workspace object in elasticsearch.
        """
        self.maxDiff = None
        params = {
            'ref': self.ref, #  reference to an AnnotatedMetagenomeAssembly object
            'sort_by': [('id', 1)],
            'start': 0,
            'limit': 10,
            'query': "16S"
        }
        ret = self.getImpl().search(self.getContext(), params)[0]
        self.assertEqual(len(ret['features']), 10)
        self.assertEqual(['16S rRNA. Bacterial SSU']*10, [r['function'] for r in ret['features']])

    # @unittest.skip('x')
    def test_search(self):
        """test the 'search' function
        NOTE: This test is tied to a version of workspace object in elasticsearch.
        """
        # self.maxDiff=None
        print('-'*80)
        print('-'*80)
        print('reference', self.ref)
        print('-'*80)
        print('-'*80)
        params = {
            'ref': self.ref, #  reference to an AnnotatedMetagenomeAssembly object
            'sort_by': [('id', 1)],
            'start': 0,
            'limit': 10
        }
        ret = self.getImpl().search(self.getContext(), params)[0]
        self.assertTrue('features' in ret, msg=f"returned: {ret.keys()}")
        self.assertTrue('start' in ret, msg=f"returned: {ret.keys()}")
        self.assertTrue('num_found' in ret, msg=f"returned: {ret.keys()}")
        self.assertTrue('query' in ret, msg=f"returned: {ret.keys()}")
        # compare_path = os.path.join(_DIR, "data", "search_test_resp_ci_43655_58_1.json")
        # with open(compare_path) as f:
        #     compare = json.load(f)
        self.assertEqual(len(ret['features']), 10)
        # self.assertEqual(ret, compare)

    # @unittest.skip('x')
    def test_get_annotated_metagenome_assembly(self):
        """test the 'get_annotated_metagenome_assembly' function"""
        appdev_ref = self.save_metagenome()
        incl = [
            'dna_size',
            'source_id',
            'genetic_code',
            'taxonomy',
            'genetic_code',
            'assembly_ref',
            'gc_content',
            'environment'
        ]
        params = {
            'ref': appdev_ref,
            'included_fields': incl,
        }
        ret = self.getImpl().get_annotated_metagenome_assembly(self.getContext(), params)[0]
        self.check_ret(ret, incl)

    # @unittest.skip('x')
    def test_search_binned_contigs(self):

        # no query
        search_params = {'ref': self.binnedcontigs_ref_1}
        ret = self.getImpl().search_binned_contigs(self.getContext(), search_params)[0]
        # pprint(ret)
        self.assertEquals(ret['num_found'], 3)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['bins']), 3)
        ret_bin_ids = [ret['bins'][0]['bin_id'], ret['bins'][1]['bin_id'], ret['bins'][2]['bin_id']]
        truth = ['out_header.001.fasta', 'out_header.002.fasta', 'out_header.003.fasta']
        self.assertEquals(tuple(sorted(ret_bin_ids)), tuple(sorted(truth)))

        # with query
        search_params = {'ref': self.binnedcontigs_ref_1, 'query': '7049'}
        ret = self.getImpl().search_binned_contigs(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 1)
        self.assertEquals(ret['query'], '7049')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['bins']), 1)
        self.assertEquals(ret['bins'][0]['bin_id'], 'out_header.002.fasta')

        # with limit
        search_params = {'ref': self.binnedcontigs_ref_1, 'limit': 2}
        ret = self.getImpl().search_binned_contigs(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 3)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['bins']), 2)
        ret_bin_ids = [ret['bins'][0]['bin_id'], ret['bins'][1]['bin_id']]
        truth = ['out_header.002.fasta', 'out_header.003.fasta']
        self.assertEquals(tuple(sorted(ret_bin_ids)), tuple(sorted(truth)))

        # with limit
        search_params = {'ref': self.binnedcontigs_ref_1, 'start': 2, 'limit': 2}
        ret = self.getImpl().search_binned_contigs(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 3)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['start'], 2)
        self.assertEquals(len(ret['bins']), 1)
        self.assertEquals(ret['bins'][0]['bin_id'], 'out_header.003.fasta')

        # sort by gc
        search_params = {'ref': self.binnedcontigs_ref_1, 'limit': 2, 'sort_by': [['gc', 0]]}
        ret = self.getImpl().search_binned_contigs(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 3)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['bins']), 2)
        self.assertEquals(ret['bins'][0]['bin_id'], 'out_header.003.fasta')
        self.assertEquals(ret['bins'][1]['bin_id'], 'out_header.001.fasta')
        search_params = {'ref': self.binnedcontigs_ref_1, 'sort_by': [['gc', 1]]}
        ret = self.getImpl().search_binned_contigs(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 3)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['bins']), 3)
        ret_bin_ids = [ret['bins'][0]['bin_id'], ret['bins'][1]['bin_id'], ret['bins'][2]['bin_id']]
        truth = ['out_header.001.fasta', 'out_header.002.fasta', 'out_header.003.fasta']
        self.assertEquals(tuple(sorted(ret_bin_ids)), tuple(sorted(truth)))

        # todo: sort by other stuff

    # @unittest.skip('x')
    def test_search_contigs_in_bin(self):

        # no query
        search_params = {'ref': self.binnedcontigs_ref_1, 'bin_id': 'out_header.002.fasta', 'limit': 5}
        ret = self.getImpl().search_contigs_in_bin(self.getContext(), search_params)[0]
        # pprint(ret)
        self.assertEquals(ret['num_found'], 369)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['bin_id'], 'out_header.002.fasta')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['contigs']), 5)
        self.assertEquals(ret['contigs'][0]['contig_id'], 'NODE_104_length_2559_cov_19.197733')

        search_params = {'ref': self.binnedcontigs_ref_1, 'bin_id': 'out_header.002.fasta', 'limit': 5, 'start': 5}
        ret = self.getImpl().search_contigs_in_bin(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 369)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['bin_id'], 'out_header.002.fasta')
        self.assertEquals(ret['start'], 5)
        self.assertEquals(len(ret['contigs']), 5)
        self.assertEquals(ret['contigs'][0]['contig_id'], 'NODE_1984_length_6967_cov_9.260514')

        # simple query
        search_params = {'ref': self.binnedcontigs_ref_1, 'query': '1678_cov_9.0399', 'bin_id': 'out_header.002.fasta'}
        ret = self.getImpl().search_contigs_in_bin(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 1)
        self.assertEquals(ret['query'], '1678_cov_9.0399')
        self.assertEquals(ret['bin_id'], 'out_header.002.fasta')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['contigs']), 1)
        self.assertEquals(ret['contigs'][0]['contig_id'], 'NODE_2131_length_1678_cov_9.039928')


        # lookup the other contigs
        search_params = {'ref': self.binnedcontigs_ref_1, 'bin_id': 'out_header.001.fasta', 'limit': 5, 'start': 5}
        ret = self.getImpl().search_contigs_in_bin(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 81)
        self.assertEquals(ret['bin_id'], 'out_header.001.fasta')

        search_params = {'ref': self.binnedcontigs_ref_1, 'bin_id': 'out_header.003.fasta', 'limit': 5, 'start': 5}
        ret = self.getImpl().search_contigs_in_bin(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 472)
        self.assertEquals(ret['bin_id'], 'out_header.003.fasta')


        # sort by length should work in both directions
        search_params = {'ref': self.binnedcontigs_ref_1, 'bin_id': 'out_header.002.fasta', 'limit': 5, 'sort_by': [['len', 0]]}
        ret = self.getImpl().search_contigs_in_bin(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 369)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['bin_id'], 'out_header.002.fasta')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['contigs']), 5)
        self.assertEquals(ret['contigs'][0]['contig_id'], 'NODE_2010_length_51632_cov_9.356794')

        search_params = {'ref': self.binnedcontigs_ref_1, 'bin_id': 'out_header.002.fasta', 'limit': 5, 'sort_by': [['len', 1]]}
        ret = self.getImpl().search_contigs_in_bin(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 369)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['bin_id'], 'out_header.002.fasta')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['contigs']), 5)
        self.assertEquals(ret['contigs'][0]['contig_id'], 'NODE_2492_length_1446_cov_9.165283')

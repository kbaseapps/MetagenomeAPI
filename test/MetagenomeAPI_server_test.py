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

from pprint import pprint


from MetagenomeUtils.MetagenomeUtilsClient import MetagenomeUtils
from AssemblyUtil.AssemblyUtilClient import AssemblyUtil

from biokbase.workspace.client import Workspace as workspaceService
from MetagenomeAPI.MetagenomeAPIImpl import MetagenomeAPI
from MetagenomeAPI.MetagenomeAPIServer import MethodContext
from MetagenomeAPI.authclient import KBaseAuth as _KBaseAuth


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

        # create some test data
        cls.au = AssemblyUtil(cls.callback_url)
        cls.mu = MetagenomeUtils(cls.callback_url)

        # building Assembly
        assembly_filename = 'small_bin_contig_file.fasta'
        cls.assembly_fasta_file_path = os.path.join(cls.scratch, assembly_filename)
        shutil.copy(os.path.join("data", assembly_filename), cls.assembly_fasta_file_path)

        assembly_params = {
            'file': {'path': cls.assembly_fasta_file_path},
            'workspace_name': cls.wsName,
            'assembly_name': 'MyAssembly'
        }
        cls.assembly_ref_1 = cls.au.save_assembly_from_fasta(assembly_params)
        print('Assembly1:' + cls.assembly_ref_1)

        # stage and build BinnedContigs data
        test_directory_name = 'test_maxbindata'
        cls.test_directory_path = os.path.join(cls.scratch, test_directory_name)
        os.makedirs(cls.test_directory_path)
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

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    def test_search_binned_contigs(self):

        # no query
        search_params = {'ref': self.binnedcontigs_ref_1}
        ret = self.getImpl().search_binned_contigs(self.getContext(), search_params)[0]
        pprint(ret)
        self.assertEquals(ret['num_found'], 3)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['bins']), 3)
        self.assertEquals(ret['bins'][0]['bin_id'], 'out_header.001.fasta')
        self.assertEquals(ret['bins'][1]['bin_id'], 'out_header.002.fasta')
        self.assertEquals(ret['bins'][2]['bin_id'], 'out_header.003.fasta')

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
        self.assertEquals(ret['bins'][0]['bin_id'], 'out_header.001.fasta')

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
        self.assertEquals(ret['bins'][0]['bin_id'], 'out_header.002.fasta')
        self.assertEquals(ret['bins'][1]['bin_id'], 'out_header.001.fasta')
        self.assertEquals(ret['bins'][2]['bin_id'], 'out_header.003.fasta')

        # todo: sort by other stuff



    def test_search_contigs_in_bin(self):

        # no query
        search_params = {'ref': self.binnedcontigs_ref_1, 'bin_id': 'out_header.002.fasta', 'limit': 5}
        ret = self.getImpl().search_contigs_in_bin(self.getContext(), search_params)[0]
        pprint(ret)
        self.assertEquals(ret['num_found'], 369)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['bin_id'], 'out_header.002.fasta')
        self.assertEquals(ret['start'], 0)
        self.assertEquals(len(ret['contigs']), 5)
        self.assertEquals(ret['contigs'][0]['contig_id'], 'NODE_2016_length_9353_cov_9.414948')

        search_params = {'ref': self.binnedcontigs_ref_1, 'bin_id': 'out_header.002.fasta', 'limit': 5, 'start': 5}
        ret = self.getImpl().search_contigs_in_bin(self.getContext(), search_params)[0]
        self.assertEquals(ret['num_found'], 369)
        self.assertEquals(ret['query'], '')
        self.assertEquals(ret['bin_id'], 'out_header.002.fasta')
        self.assertEquals(ret['start'], 5)
        self.assertEquals(len(ret['contigs']), 5)
        self.assertEquals(ret['contigs'][0]['contig_id'], 'NODE_2311_length_2281_cov_9.117930')

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

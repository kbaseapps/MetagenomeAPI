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
        #if hasattr(cls, 'ws_info'):
        #    cls.wsClient.delete_workspace({'workspace': cls.wsName})
        #    print('Test workspace was deleted')
        pass

    def getWsClient(self):
        return self.__class__.wsClient

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    def test_search_binned_contigs(self):


        #search_params = {'ref': cls.binn 
        #                }

        #ret = self.getImpl().search_binnedcontigs({

        #    })


        # Prepare test objects in workspace if needed using
        # self.getWsClient().save_objects({'workspace': self.getWsName(),
        #                                  'objects': []})
        #
        # Run your method by
        # ret = self.getImpl().your_method(self.getContext(), parameters...)
        #
        # Check returned data with
        # self.assertEqual(ret[...], ...) or other unittest methods
        pass

# -*- coding: utf-8 -*-
#BEGIN_HEADER

from MetagenomeAPI.BinnedContigsIndexer import BinnedContigsIndexer
from Workspace.WorkspaceClient import Workspace
from MetagenomeAPI.AMAUtils import AMAUtils
from MetagenomeAPI.MetagenomeSearchUtils import MetagenomeSearchUtils
#END_HEADER


class MetagenomeAPI:
    '''
    Module Name:
    MetagenomeAPI

    Module Description:
    
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "1.0.3"
    GIT_URL = "https://github.com/slebras/MetagenomeAPI.git"
    GIT_COMMIT_HASH = "0dee8acaea89df8590158a73290f8fce329d8979"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.indexer = BinnedContigsIndexer(config)
        self.msu = MetagenomeSearchUtils(config)
        self.config = config
        #END_CONSTRUCTOR
        pass


    def search_binned_contigs(self, ctx, params):
        """
        :param params: instance of type "SearchBinnedContigsOptions"
           (num_found - optional field which when set informs that there is
           no need to perform full scan in order to count this value because
           it was already done before; please don't set this value with 0 or
           any guessed number if you didn't get right value previously.) ->
           structure: parameter "ref" of String, parameter "query" of String,
           parameter "sort_by" of list of type "column_sorting" -> tuple of
           size 2: parameter "column" of String, parameter "ascending" of
           type "boolean" (Indicates true or false values, false = 0, true =
           1 @range [0,1]), parameter "start" of Long, parameter "limit" of
           Long, parameter "num_found" of Long
        :returns: instance of type "SearchBinnedContigsResult" (num_found -
           number of all items found in query search (with only part of it
           returned in "bins" list).) -> structure: parameter "query" of
           String, parameter "start" of Long, parameter "bins" of list of
           type "ContigBinData" (bin_id          - id of the bin n_contigs   
           - number of contigs in this bin gc              - GC content over
           all the contigs sum_contig_len  - (bp) total length of the contigs
           cov             - coverage over the bin (if available, may be
           null)) -> structure: parameter "bin_id" of String, parameter
           "n_contigs" of Long, parameter "gc" of Double, parameter
           "sum_contig_len" of Long, parameter "cov" of Double, parameter
           "num_found" of Long
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN search_binned_contigs
        result = self.indexer.search_binned_contigs(ctx["token"],
                                                    params.get("ref", None),
                                                    params.get("query", None),
                                                    params.get("sort_by", None),
                                                    params.get("start", None),
                                                    params.get("limit", None),
                                                    params.get("num_found", None))
        #END search_binned_contigs

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method search_binned_contigs return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]

    def search_contigs_in_bin(self, ctx, params):
        """
        :param params: instance of type "SearchContigsInBin" (num_found -
           optional field which when set informs that there is no need to
           perform full scan in order to count this value because it was
           already done before; please don't set this value with 0 or any
           guessed number if you didn't get right value previously.) ->
           structure: parameter "ref" of String, parameter "bin_id" of
           String, parameter "query" of String, parameter "sort_by" of list
           of type "column_sorting" -> tuple of size 2: parameter "column" of
           String, parameter "ascending" of type "boolean" (Indicates true or
           false values, false = 0, true = 1 @range [0,1]), parameter "start"
           of Long, parameter "limit" of Long, parameter "num_found" of Long
        :returns: instance of type "SearchContigsInBinResult" (num_found -
           number of all items found in query search (with only part of it
           returned in "bins" list).) -> structure: parameter "query" of
           String, parameter "bin_id" of String, parameter "start" of Long,
           parameter "contigs" of list of type "ContigInBin" (contig_id      
           - id of the contig len             - (bp) length of the contig gc 
           - GC content over the contig cov             - coverage over the
           contig (if available, may be null)) -> structure: parameter
           "contig_id" of String, parameter "len" of Long, parameter "gc" of
           Double, parameter "cov" of Double, parameter "num_found" of Long
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN search_contigs_in_bin
        result = self.indexer.search_contigs_in_bin(ctx["token"],
                                                    params.get("ref", None),
                                                    params.get("bin_id", None),
                                                    params.get("query", None),
                                                    params.get("sort_by", None),
                                                    params.get("start", None),
                                                    params.get("limit", None),
                                                    params.get("num_found", None))
        #END search_contigs_in_bin

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method search_contigs_in_bin return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]

    def get_annotated_metagenome_assembly(self, ctx, params):
        """
        :param params: instance of type
           "getAnnotatedMetagenomeAssemblyParams" (ref - workspace reference
           to AnnotatedMetagenomeAssembly Object included_fields - The fields
           to include from the Object included_feature_fields -) ->
           structure: parameter "ref" of String, parameter "included_fields"
           of list of String
        :returns: instance of type "getAnnotatedMetagenomeAssemblyOutput" ->
           structure: parameter "genomes" of list of unspecified object
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN get_annotated_metagenome_assembly
        ws = Workspace(self.config['workspace-url'], token=ctx['token'])
        ama_utils = AMAUtils(ws)
        output = ama_utils.get_annotated_metagenome_assembly(params)
        #END get_annotated_metagenome_assembly

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method get_annotated_metagenome_assembly return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]

    def search(self, ctx, params):
        """
        :param params: instance of type "SearchOptions" (query: user provided
           input string to prefix search against 'functions',
           'functional_descriptions', 'id', and 'type' fields of the
           metagenome features. ref:
           `KBaseMetagenomes.AnnotatedMetagenomeAssembly` workspace object
           reference sort_by: list of tuples by which to sort by, ex:
           [("elasticsearch ", ascend bool), ...] start: integer start of
           pagination limit: integer end of pagination) -> structure:
           parameter "query" of String, parameter "ref" of String, parameter
           "sort_by" of list of type "column_sorting" -> tuple of size 2:
           parameter "column" of String, parameter "ascending" of type
           "boolean" (Indicates true or false values, false = 0, true = 1
           @range [0,1]), parameter "start" of Long, parameter "limit" of Long
        :returns: instance of type "SearchResult" (num_found - number of all
           items found in query search (with only part of it returned in
           "features" list). query: the query used on the Search2API start:
           integer index start of pagination features: list of feature
           information.) -> structure: parameter "query" of String, parameter
           "start" of Long, parameter "features" of list of type
           "FeatureData" (aliases - mapping from alias name (key) to set of
           alias sources (value) global_location - this is location-related
           properties that are under sorting whereas items in "location"
           array are not, feature_array - field recording which array a
           feature is located in (features, mrnas, cdss, non_coding_features)
           feature_idx - field keeping the position of feature in its array
           in a Genome object, ontology_terms - mapping from term ID (key) to
           term name (value).) -> structure: parameter "feature_id" of
           String, parameter "aliases" of mapping from String to list of
           String, parameter "function" of String, parameter "location" of
           list of type "Location" -> structure: parameter "contig_id" of
           String, parameter "start" of Long, parameter "strand" of String,
           parameter "length" of Long, parameter "feature_type" of String,
           parameter "global_location" of type "Location" -> structure:
           parameter "contig_id" of String, parameter "start" of Long,
           parameter "strand" of String, parameter "length" of Long,
           parameter "feature_array" of String, parameter "feature_idx" of
           Long, parameter "ontology_terms" of mapping from String to String,
           parameter "num_found" of Long
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN search
        result = self.msu.search(ctx['token'], params.get('ref'), params.get('start'),
                                 params.get('limit'), params.get('sort_by'), params.get('query'))
        #END search

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method search return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]

    def search_region(self, ctx, params):
        """
        :param params: instance of type "SearchRegionOptions" (ref:
           `KBaseMetagenomes.AnnotatedMetagenomeAssembly` workspace object
           reference contig_id: id of contig to search around. region_start:
           integer start of contig context to search around. region_length:
           integer lenght of contig context to search around. page_start:
           integer start of pagination page_limit: integer end of pagination
           sort_by: list of tuples by which to sort by, ex: [("elasticsearch
           ", ascend bool), ...]) -> structure: parameter "ref" of String,
           parameter "contig_id" of String, parameter "region_start" of Long,
           parameter "region_length" of Long, parameter "page_start" of Long,
           parameter "page_limit" of Long, parameter "sort_by" of list of
           type "column_sorting" -> tuple of size 2: parameter "column" of
           String, parameter "ascending" of type "boolean" (Indicates true or
           false values, false = 0, true = 1 @range [0,1])
        :returns: instance of type "SearchRegionResult" (contig_id: id of
           contig to search around, (same as input). region_start: integer
           start of contig context to search around, (same as input).
           region_length: integer lenght of contig context to search around,
           (same as input). start: integer start of pagination features: list
           of feature information. num_found: the total number of matches
           with the query (without pagination)) -> structure: parameter
           "contig_id" of String, parameter "region_start" of Long, parameter
           "region_length" of Long, parameter "start" of Long, parameter
           "features" of list of type "FeatureData" (aliases - mapping from
           alias name (key) to set of alias sources (value) global_location -
           this is location-related properties that are under sorting whereas
           items in "location" array are not, feature_array - field recording
           which array a feature is located in (features, mrnas, cdss,
           non_coding_features) feature_idx - field keeping the position of
           feature in its array in a Genome object, ontology_terms - mapping
           from term ID (key) to term name (value).) -> structure: parameter
           "feature_id" of String, parameter "aliases" of mapping from String
           to list of String, parameter "function" of String, parameter
           "location" of list of type "Location" -> structure: parameter
           "contig_id" of String, parameter "start" of Long, parameter
           "strand" of String, parameter "length" of Long, parameter
           "feature_type" of String, parameter "global_location" of type
           "Location" -> structure: parameter "contig_id" of String,
           parameter "start" of Long, parameter "strand" of String, parameter
           "length" of Long, parameter "feature_array" of String, parameter
           "feature_idx" of Long, parameter "ontology_terms" of mapping from
           String to String, parameter "num_found" of Long
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN search_region
        result = self.msu.search_region(ctx["token"],
                                        params.get("ref", None),
                                        params.get("contig_id", None),
                                        params.get("region_start", None),
                                        params.get("region_length", None),
                                        params.get("page_start", None),
                                        params.get("page_limit", None),
                                        params.get("sort_by", None))
        #END search_region

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method search_region return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]

    def search_contigs(self, ctx, params):
        """
        :param params: instance of type "SearchContigsOptions" (ref -
           `KBaseMetagenomes.AnnotatedMetagenomeAssembly` workspace object
           reference start - integer start of pagination limit - integer
           limit of pagination sort_by - tuple by which to sort by and string
           component must be one of ("length/contig_id/feature_count", ascend
           bool)) -> structure: parameter "ref" of String, parameter "start"
           of Long, parameter "limit" of Long, parameter "sort_by" of type
           "column_sorting" -> tuple of size 2: parameter "column" of String,
           parameter "ascending" of type "boolean" (Indicates true or false
           values, false = 0, true = 1 @range [0,1])
        :returns: instance of type "SearchContigsResult" (num_found - number
           of contigs found in total, start - start of the pagination contigs
           - list of contig individual contig information) -> structure:
           parameter "num_found" of Long, parameter "start" of Long,
           parameter "contigs" of list of type "contig" (contig_id -
           identifier of contig feature_count - number of features associated
           with contig length - the dna sequence length of the contig) ->
           structure: parameter "contig_id" of String, parameter
           "feature_count" of Long, parameter "length" of Long
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN search_contigs
        if 'ref' not in params:
          raise RuntimeError(f"'ref' argument required for search_contigs")
        if 'limit' not in params:
          raise RuntimeError(f"'limit' argument required for search_contigs")
        if 'start' not in params:
          raise RuntimeError(f"'start' argument required for search_contigs")
        if not params.get('sort_by'):
          sort_by = ('length', 0)
        else:
          sort_by = params['sort_by']
          if sort_by[0] not in ['length', 'contig_id', 'feature_count']:
            raise RuntimeError("'sort_by' argument can only contain one "
                               "of 'length', 'contig_id', or 'feature_count'")

        ws = Workspace(self.config['workspace-url'], token=ctx['token'])
        ama_utils = AMAUtils(ws)
        params['included_fields'] = ['contig_ids', 'contig_lengths']
        ama = ama_utils.get_annotated_metagenome_assembly(params)['genomes'][0]
        data = ama['data']
        if len(data['contig_ids']) != len(data['contig_lengths']):
          raise RuntimeError(f"contig ids (size: {len(data['contig_ids'])}) and contig "
                             f"lengths (size: {len(data['contig_lengths'])}) sizes do not match.")
        contig_ids = data['contig_ids']
        contig_lengths = data['contig_lengths']

        range_start = params['start']
        range_end = params['start'] + params['limit']

        if sort_by[0] == 'contig_id' and sort_by[1] == 0:
          contig_ids, contig_lengths = (list(t) for t in zip(*sorted(zip(contig_ids, contig_lengths), reverse=True)))
        elif sort_by[0] == 'length':
          contig_lengths, contig_ids = (list(t) for t in zip(*sorted(zip(contig_lengths, contig_ids), reverse=sort_by[1] == 0)))
        elif sort_by[0] == 'feature_count':
          # first sort by lengths
          contig_lengths, contig_ids = (list(t) for t in zip(*sorted(zip(contig_lengths, contig_ids), reverse=sort_by[1] == 0)))
          width = int(params['limit']/2)

          range_start = max(range_start - width, 0)
          range_end = min(range_end + width, len(contig_ids))

        # get feature_counts
        feature_counts = self.msu.search_contig_feature_counts(ctx["token"],
                                params.get("ref"),
                                len(contig_ids))

        contigs = [
          {
            "contig_id": contig_ids[i],
            "feature_count": feature_counts.get(contig_ids[i], 0),
            "length": contig_lengths[i]
          }
          for i in range(range_start, range_end)
        ]
        # for i in range(range_start, range_end):
        #   # not a great solution, but works for now.
        #   if i < len(contig_ids) or i >= 0:
        #     contig_id = contig_ids[i]
        #     contig_length = contig_lengths[i]
        #     feature_count = self.msu.search_contig_feature_count(ctx["token"],
        #                                   params.get("ref"),
        #                                   contig_id,
        #                                   params.get("start"),
        #                                   params.get("limit"))
        #     contig_data = {
        #       "contig_id": contig_id,
        #       "feature_count": feature_count,
        #       "length": contig_length
        #     }
        #     contigs.append(contig_data)

        # not ideal, but will work for now...
        if sort_by[0] == 'feature_count':
          contigs = sorted(contigs, key=lambda x: x['feature_count'], reverse=sort_by[1] == 0)
          contigs = contigs[max(0, params['start']):min(len(contigs),params['start'] + params['limit'])]

        result =  {
          "contigs": contigs,
          "num_found": len(data['contig_ids']),
          "start": params['start']
        }
        #END search_contigs

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method search_contigs return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]

    def get_contig_info(self, ctx, params):
        """
        :param params: instance of type "GetContigInfoParams" -> structure:
           parameter "ref" of String, parameter "contig_id" of String
        :returns: instance of type "GetContigInfoResult" -> structure:
           parameter "contig" of type "contig" (contig_id - identifier of
           contig feature_count - number of features associated with contig
           length - the dna sequence length of the contig) -> structure:
           parameter "contig_id" of String, parameter "feature_count" of
           Long, parameter "length" of Long
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN get_contig_info
        if 'ref' not in params:
          raise RuntimeError(f"'ref' argument required for get_contig_info")
        if 'contig_id' not in params:
          raise RuntimeError(f"'contig_id' argument required for get_contig_info")
        contig_id = params['contig_id']
        ws = Workspace(self.config['workspace-url'], token=ctx['token'])
        ama_utils = AMAUtils(ws)
        params['included_fields'] = ['contig_ids', 'contig_lengths']
        data = ama_utils.get_annotated_metagenome_assembly(params)['genomes'][0]['data']
        contig_ids = data['contig_ids']
        contig_lengths = data['contig_lengths']
        for i, c in enumerate(contig_ids):
          if c == contig_id:
            length = contig_lengths[i]
            break
        feature_count = self.msu.search_contig_feature_count(ctx["token"],
                                params.get("ref"),
                                contig_id)
        result = {
          'contig': {
            "contig_id": contig_id,
            "length": length,
            "feature_count": feature_count
          }
        }
        #END get_contig_info

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method get_contig_info return value ' +
                             'result is not type dict as required.')
        # return the results
        return [result]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]

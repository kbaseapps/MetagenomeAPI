# -*- coding: utf-8 -*-
#BEGIN_HEADER
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
    VERSION = "0.0.1"
    GIT_URL = ""
    GIT_COMMIT_HASH = "HEAD"

    #BEGIN_CLASS_HEADER
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
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
           parameter "bins" of list of type "ContigBinData" (bin_id         
           - id of the bin n_contigs       - number of contigs in this bin gc
           - GC content over all the contigs sum_contig_len  - (bp) total
           length of the contigs cov             - coverage over the bin (if
           available, may be null)) -> structure: parameter "bin_id" of
           String, parameter "n_contigs" of Long, parameter "gc" of Double,
           parameter "sum_contig_len" of Long, parameter "cov" of Double,
           parameter "num_found" of Long
        """
        # ctx is the context object
        # return variables are: result
        #BEGIN search_contigs_in_bin
        #END search_contigs_in_bin

        # At some point might do deeper type checking...
        if not isinstance(result, dict):
            raise ValueError('Method search_contigs_in_bin return value ' +
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

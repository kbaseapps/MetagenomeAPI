/*  */
module MetagenomeAPI {
    
    /*
        Indicates true or false values, false = 0, true = 1
        @range [0,1]
    */
    typedef int boolean;

    typedef tuple<string column, boolean ascending> column_sorting;


    /*
        num_found - optional field which when set informs that there
            is no need to perform full scan in order to count this
            value because it was already done before; please don't
            set this value with 0 or any guessed number if you didn't 
            get right value previously.
    */
    typedef structure {
        string ref;
        string query;
        list<column_sorting> sort_by;
        int start;
        int limit;
        int num_found;
    } SearchBinnedContigsOptions;

    /*
        bin_id          - id of the bin
        n_contigs       - number of contigs in this bin
        gc              - GC content over all the contigs
        sum_contig_len  - (bp) total length of the contigs
        cov             - coverage over the bin (if available, may be null)
    */
    typedef structure {
        string bin_id;
        int n_contigs;
        float gc;
        int sum_contig_len;
        float cov;
    } ContigBinData;

    /*
        num_found - number of all items found in query search (with 
            only part of it returned in "bins" list).
    */
    typedef structure {
        string query;
        int start;
        list<ContigBinData> bins;
        int num_found;
    } SearchBinnedContigsResult;

    funcdef search_binned_contigs(SearchBinnedContigsOptions params) 
        returns (SearchBinnedContigsResult result) authentication optional;




    /*
        num_found - optional field which when set informs that there
            is no need to perform full scan in order to count this
            value because it was already done before; please don't
            set this value with 0 or any guessed number if you didn't 
            get right value previously.
    */
    typedef structure {
        string ref;
        string bin_id;
        string query;
        list<column_sorting> sort_by;
        int start;
        int limit;
        int num_found;
    } SearchContigsInBin;

    /*
        contig_id       - id of the contig
        len             - (bp) length of the contig
        gc              - GC content over the contig
        cov             - coverage over the contig (if available, may be null)
    */
    typedef structure {
        string contig_id;
        int len;
        float gc;
        float cov;
    } ContigInBin;

    /*
        num_found - number of all items found in query search (with 
            only part of it returned in "bins" list).
    */
    typedef structure {
        string query;
        string bin_id;
        int start;
        list<ContigInBin> contigs;
        int num_found;
    } SearchContigsInBinResult;

    funcdef search_contigs_in_bin(SearchContigsInBin params)
        returns (SearchContigsInBinResult result) authentication optional;


    /*
      ref - workspace reference to AnnotatedMetagenomeAssembly Object
      included_fields - The fields to include from the Object
      included_feature_fields - 

    */
    typedef structure {
      string ref;
      list<string> included_fields;
    } getAnnotatedMetagenomeAssemblyParams;

    typedef structure {
      list<UnspecifiedObject> genomes;
    } getAnnotatedMetagenomeAssemblyOutput;

    funcdef get_annotated_metagenome_assembly(getAnnotatedMetagenomeAssemblyParams params)
        returns (getAnnotatedMetagenomeAssemblyOutput output) authentication required;

};

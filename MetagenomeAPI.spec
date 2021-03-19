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
        returns (getAnnotatedMetagenomeAssemblyOutput output) authentication optional;

    /*
        query: user provided input string to prefix search against 'functions', 'functional_descriptions', 'id', and 'type' fields of the metagenome features.
        ref: `KBaseMetagenomes.AnnotatedMetagenomeAssembly` workspace object reference
        sort_by: list of tuples by which to sort by, ex: [("elasticsearch ", ascend bool), ...]
        start: integer start of pagination
        limit: integer end of pagination
    */

    typedef structure {
        string query;
        string ref;
        list<column_sorting> sort_by;
        int start;
        int limit;
    } SearchOptions;

    typedef structure {
        string contig_id;
        int start;
        string strand;
        int length;
    } Location;

    /*
        aliases - mapping from alias name (key) to set of alias sources (value)
        global_location - this is location-related properties that are
            under sorting whereas items in "location" array are not,
        feature_array - field recording which array a feature is located in
            (features, mrnas, cdss, non_coding_features)
        feature_idx - field keeping the position of feature in its array in a
            Genome object,
        ontology_terms - mapping from term ID (key) to term name (value).
    */

    typedef structure {
        string feature_id;
        string dna_sequence;
        list<string> warnings;
        string parent_gene;
        int size;
        list<string> functional_descriptions;
        mapping<string, list<string>> aliases;
        string function;
        list<Location> location;
        string feature_type;
        Location global_location;
        string feature_array;
        int feature_idx;
        mapping<string, string> ontology_terms;
    } FeatureData;

    /*
        num_found - number of all items found in query search (with only part of it returned in "features" list).
        query: the query used on the Search2API
        start: integer index start of pagination
        features: list of feature information.
    */

    typedef structure {
        string query;
        int start;
        list<FeatureData> features;
        int num_found;
    } SearchResult;

    funcdef search(SearchOptions params) returns (SearchResult result) authentication optional;

    /*
        ref: `KBaseMetagenomes.AnnotatedMetagenomeAssembly` workspace object reference
        contig_id: id of contig to search around.
        region_start: integer start of contig context to search around.
        region_length: integer lenght of contig context to search around.
        page_start: integer start of pagination
        page_limit: integer end of pagination
        sort_by: list of tuples by which to sort by, ex: [("elasticsearch ", ascend bool), ...]
    */

    typedef structure {
        string ref;
        string contig_id;
        int region_start;
        int region_length;
        int page_start;
        int page_limit;
        list<column_sorting> sort_by;
    } SearchRegionOptions;

    /*
        contig_id: id of contig to search around, (same as input).
        region_start: integer start of contig context to search around, (same as input).
        region_length: integer lenght of contig context to search around, (same as input).
        start: integer start of pagination
        features: list of feature information.
        num_found: the total number of matches with the query (without pagination)
    */

    typedef structure {
        string contig_id;
        int region_start;
        int region_length;
        int start;
        list<FeatureData> features;
        int num_found;
    } SearchRegionResult;

    funcdef search_region(SearchRegionOptions params) returns (SearchRegionResult result) authentication optional;

    /*
        ref - `KBaseMetagenomes.AnnotatedMetagenomeAssembly` workspace object reference
        start - integer start of pagination 
        limit - integer limit of pagination
        sort_by - tuple by which to sort by and string component must be one of ("length/contig_id/feature_count", ascend bool) 
    */

    typedef structure {
        string ref;
        int start;
        int limit;
        column_sorting sort_by;
    } SearchContigsOptions;

    /*
        contig_id - identifier of contig
        feature_count - number of features associated with contig
        length - the dna sequence length of the contig
    */

    typedef structure {
        string contig_id;
        int feature_count;
        int length;
    } contig;

    /*
        num_found - number of contigs found in total,
        start - start of the pagination
        contigs - list of contig individual contig information
    */

    typedef structure {
        int num_found;
        int start;
        list<contig> contigs;
    } SearchContigsResult;

    funcdef search_contigs(SearchContigsOptions params) returns (SearchContigsResult result) authentication optional;

    typedef structure {
        string ref;
        string contig_id;
    } GetContigInfoParams;

    typedef structure {
        contig contig;
    } GetContigInfoResult;

    funcdef get_contig_info(GetContigInfoParams params) returns (GetContigInfoResult result) authentication optional;

    typedef structure {
        string ref;
    } GetFeatureTypeCountsParams;

    typedef structure {
        mapping<string, int> feature_type_counts;
    } GetFeatureTypeCountsResult;

    funcdef get_feature_type_counts(GetFeatureTypeCountsParams params) returns (GetFeatureTypeCountsResult result) authentication optional;

};

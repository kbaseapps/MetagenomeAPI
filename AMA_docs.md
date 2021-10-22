# Annotated Metagenome Assembly Documentation
KBaseMetagenomes.AnnotatedMetagenomeAssembly - Annotated Metagenome Assembly (AMA) documentation

There are several quirks to the AMA object that make it a unique datatype in the KBase environment. Because of the size of the files associated with this object type, and the workspace limitations (objects must be <1GB in size), the AMA objects handle their associated data uniquely.

## Datatype spec
The AMA objects [datatype specification](https://ci.kbase.us/#spec/type/KBaseMetagenomes.AnnotatedMetagenomeAssembly) is based on the datatype specification of the Genome Object ([KBaseGenomes.Genome](https://ci.kbase.us/#spec/type/KBaseGenomes.Genome)). The AMA type spec contains a subset of the information that the Genome Object contains, primarily excluding information that refers to a specific organism, like taxonomic info. The general structure and naming is directly pulled from the Genome spec and the AMA object actually shares a lot of import code with the Genome. The most notable difference is how the AMA features are stored when compared to Genome Features.

### How Features are stored
There is a major difference in how the AMA objects store features and how the Genome object stores features. the `features_handle_ref` field in the AMA contains a reference to JSON file stored in shock (the KBase file store) which contains all the features, stored in the same fashion as the features in the Genome object are stored.

Heres an example of what the AMA feature blob may look like:
```JSON
[{
 	"id": "",
 	"type": "CDS",
 	"location": [["c1", 1, "+", 12], ["c2", 22, "-", 35]],
	"functions": [],
	"ontology_terms": {"ENVO:000": {"": [1,2,3]}},
	"note": "",
	"md5": "",
	"children": [],
	"flags": [""],
	"warnings": [],
	"inference_data": [{"category": "","type": "","evidence": ""}],
	"dna_sequence": "AAATTGGTTCTATGCGCGCTTGGA.........",
	"dna_sequence_length": 299,
	"aliases": [],
	"db_xrefs": []
}]
```

## Relevant functions
### MetagenomeAPI
the MetagenomeAPI functions are designed to support the front-end viewers of the AMA objects. The functions that support AMA's are as follows:
[`get_annotated_metagenome_assembly`](https://github.com/kbaseapps/MetagenomeAPI/blob/4adf44352df89025d2811aa6e64bbfbbd0358f10/MetagenomeAPI.spec#L119) - this function is a wrapper for retrieving the AMA object from the workspace. it mainly exists to support the narrative data viewer which is heavily copied from the genome narrative viewer.

[`search`](https://github.com/kbaseapps/MetagenomeAPI/blob/4adf44352df89025d2811aa6e64bbfbbd0358f10/MetagenomeAPI.spec#L187) - this function allows users to search for features. it is primarly used to support the pagination and term search functions (only against, the 'functions', 'functional_descriptions', 'id', and 'type' fields) of the feature viewer functionality of AMA viewers.

[`search_region`](https://github.com/kbaseapps/MetagenomeAPI/blob/4adf44352df89025d2811aa6e64bbfbbd0358f10/MetagenomeAPI.spec#L227) - this functions allows users to search features within a dna region as defined by a start and stop position. It functions similarily to the `search` function but does not allow users to search terms. It supports the GUI's that show feature locations.

[`search_contigs`](https://github.com/kbaseapps/MetagenomeAPI/blob/4adf44352df89025d2811aa6e64bbfbbd0358f10/MetagenomeAPI.spec#L267) - this functions allows users to search based on contigs. It functions similarily to the `search` function but does not allow users to search terms. It supports the viewers that show a contig centric view.

[`get_contig_info`](https://github.com/kbaseapps/MetagenomeAPI/blob/4adf44352df89025d2811aa6e64bbfbbd0358f10/MetagenomeAPI.spec#L278) - Gets metadata about a particular contig by id. Compiled primarly from aggregated info from elasticsearch.

[`get_feature_type_counts`](https://github.com/kbaseapps/MetagenomeAPI/blob/4adf44352df89025d2811aa6e64bbfbbd0358f10/MetagenomeAPI.spec#L288) - Gets metadata about the number and type of features in an AMA object.


### MetagenomeUtils
the MetagenomeUtils functions are designed for general support of the AMA objects. The functions that support AMA's are as follows:

[`get_annotated_metagenome_assembly`](https://github.com/kbaseapps/MetagenomeUtils/blob/54c6bc35fa899823966a3b0ab972e2cdfd2a36a2/MetagenomeUtils.spec#L315) - A wrapper function for getting the AMA object from the workspace

[`get_annotated_metagenome_assembly_features`](https://github.com/kbaseapps/MetagenomeUtils/blob/54c6bc35fa899823966a3b0ab972e2cdfd2a36a2/MetagenomeUtils.spec#L346) -  **IMPORTANT**: This function shoud be used everytime features are needed. Because the storage method of the features is unconventional and relies in file format, using this function will allow for changes down the road. It currently returns all features but that should perhaps be changed.

## Index Runner - indexer set up
The AMA object has [its own indexer](https://github.com/kbase/index_runner/blob/develop/src/index_runner/es_indexers/annotated_metagenome_assembly.py) in the Index Runner repository that indexes both the workspace object itself and its associated features. Because of the the size of the AMA objects and features, there is a separate set of Indexers that are reserved for indexing these objects. 

## Areas to improve
The major area to improve the AMA object is in how the features are stored. Coming up with a better feature solution that could take some of the load off of the Index Runner would greatly benefit this workflow and others.

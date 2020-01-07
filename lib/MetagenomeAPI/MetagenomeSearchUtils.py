# -*- coding: utf-8 -*-
import json
import os
import requests
import time


class MetagenomeSearchUtils:

    def __init__(self, config):
        if config.get('search-url'):
            self.search_url = config.get('search-url')
        else:
            self.search_url = config.get('kbase-endpoint') + '/searchapi2/rpc'
        # check if server is active.
        resp = requests.get(config.get('kbase-endpoint') + '/searchapi2')
        if not resp.ok:
            self.status_good = False
        resp_json = resp.json()
        if resp_json.get('status'):
            if resp_json['status'] == 'ok':
                self.status_good = True
            else:
                self.status_good = False
        else:
            self.status_good = False
        self.debug = config.get("debug") == "1"
        self.max_sort_mem_size = 250000

    def search_contig_feature_count(self, token, ref, contig_id):
        """
        Given a contig, find the number of features it has
        token         - workspace authentication token
        ref           - workspace object reference
        contig_id     - contig id of contig to query around
        """
        extra_must = [{'term': {'contig_ids': contig_id}}]
        # limit and size set to 1 to avoid needlessly unenforcing 'terminate_after' in search_api
        ret = self._elastic_query(token, ref, 1, 1, [('id', 1)], extra_must=extra_must)
        # this will correspond to 'feature_count'
        return ret['num_found']

    def search_contig_feature_counts(self, token, ref, num_contigs):
        """
        Finds all contig feature counts for a provided AnnotatedMetagneomeAssembly object reference
        token         - workspace authentication token
        ref           - workspace object reference
        num_contigs   - number of contigs in object.
        """
        if not self.status_good:
            return {}
        (workspace_id, object_id, version) = ref.split('/')
        # we use namespace 'WSVER' for versioned elasticsearch index.
        ama_id = f'WSVER::{workspace_id}:{object_id}:{version}'

        headers = {"Authorization": token}        
        params = {
            "method": "search_objects",
            "params": {
                "query": {  # "term": {"parent_id": ama_id}},
                    "bool": {
                        "must": [{"term": {"parent_id": ama_id}}] 
                    }
                },
                "indexes": ["annotated_metagenome_assembly_features_version"],
                "size": 0,
                "aggs": {
                    "group_by_state": {
                        "terms": {
                            "field": "contig_ids",
                            "size": num_contigs
                        },
                    }
                }
            }
        }
        resp = requests.post(self.search_url, headers=headers, data=json.dumps(params))
        if not resp.ok:
            raise Exception(f"Not able to complete search request against {self.search_url} "
                            f"with parameters: {json.dumps(params)} \nResponse body: {resp.text}")
        respj = resp.json()
        return {
            b['key']: b['doc_count'] for b in respj['aggregations']['group_by_state']['buckets']
        }

    def search_region(self, token, ref, contig_id, region_start, region_length, start, limit, sort_by):
        """
        Search a region of features in a given contig
        token         - workspace authentication token
        ref           - workspace object reference
        contig_id     - contig id of contig to query around
        region_start  - integer position of the start of the region to search for
        region_length - integer lenght of the region to search for
        start         - elasticsearch page start delimeter
        limit         - elasticserch page limit
        sort_by       - list of tuples of (field to sort by, ascending bool) for elasticsearch
        """
        if start is None:
            start = 0
        if limit is None:
            limit = 100
        if sort_by is None:
            sort_by = [('starts', 1), ('stops', 1)]

        t1 = time.time()
        extra_must = [
            {"range": {
                    "starts": {
                        "gte": int(region_start)
                    }
                }
            },{"range": {
                    "stops": {
                        "lte": int(region_start + region_length)
                    }
                }
            },{"term": {"contig_ids": contig_id}}
        ]
        ret = self._elastic_query(token, ref, limit, start, sort_by, extra_must=extra_must)
        # not sure we need to include any of these
        ret['region_start'] = region_start
        ret['contig_id'] = contig_id
        ret['region_length'] = region_length
        if self.debug:
            print(("    (overall-time=" + str(time.time() - t1) + ")"))
        return ret

    def search(self, token, ref, start, limit, sort_by, query):
        """
        Search features against one Annotated Metagenome Assembly, with or without a query string.
        token   - workspace authentication token
        ref     - workspace object reference
        start   - elasticsearch page start delimiter
        limit   - elasticsearch page limit
        sort_by - list of tuples of (field to sort by, ascending bool) for elasticsearch
        query   - text to prefix search on all fields.
        """
        if start is None:
            start = 0
        if limit is None:
            limit = 50
        if sort_by is None:
            sort_by = [('id', 1)]

        extra_must = []
        if query:
            fields = ["functions", "functional_descriptions", "id", "type"]
            shoulds = []
            for field in fields:
                shoulds.append({
                    "prefix": {field: {"value": query}}
                })
            extra_must.append({'bool': {'should': shoulds}})

        t1 = time.time()
        ret = self._elastic_query(token, ref, limit, start, sort_by, extra_must=extra_must)
        if self.debug:
            print(("    (overall-time=" + str(time.time() - t1) + ")"))
        return ret

    def _elastic_query(self, token, ref, limit, start, sort_by, extra_must=[], aggs=None):
        """
        Perform the query against the Search API 2, to get results from elasticsearch.
        token      - workspace authentication token
        ref        - workspace object reference
        limit      - elasticsearch page limit
        start      - elasticsearch page start delimiter
        sort_by    - list of tuples of (field to sort by, ascending bool) for elasticsearch
        extra_must - list of additional elasticsearch eql json blobs to include as query
        """
        (workspace_id, object_id, version) = ref.split('/')
        # we use namespace 'WSVER' for versioned elasticsearch index.
        ama_id = f'WSVER::{workspace_id}:{object_id}:{version}'

        headers = {"Authorization": token}
        params = {
            "method": "search_objects",
            "params": {
                "query": {
                    "bool": {
                        "must": [{"term": {"parent_id": ama_id}}] + extra_must 
                    }
                },
                "indexes": ["annotated_metagenome_assembly_features_version"],
                "from": start,
                "size": limit,
                "sort": [{s[0]: {"order": "asc" if s[1] else "desc"}} for s in sort_by]
            }
        }
        if aggs:
            params['aggs'] = aggs
        # if self.debug:
        # print(f"querying {self.search_url}, with params: {json.dumps(params)}")
        resp = requests.post(self.search_url, headers=headers, data=json.dumps(params))
        if not resp.ok:
            raise Exception(f"Not able to complete search request against {self.search_url} "
                            f"with parameters: {json.dumps(params)} \nResponse body: {resp.text}")
        respj = resp.json()
        return self._process_resp(respj, start, params)

    def _process_resp(self, resp, start, params):
        """
        Format the response
        resp   - json response from search api
        start  - integer start of pagination
        params - parameters used to search against SearchAPI2
        """
        if resp.get('hits') and resp['hits'].get('hits'):
            hits = resp['hits']['hits']
            return {
                "num_found": int(resp['hits']['total']),
                "start": start,
                "query": params,
                "features": [self._process_feature(h['_source']) for h in hits]
            }
        elif resp.get('hits'):
            return {
                "num_found": int(resp['hits']['total']),
                "start": start,
                "query": params,
                "features": []
            }
        else:
            # only raise if no hits found at highest level.
            raise RuntimeError(f"no 'hits' with params {json.dumps(params)}\n in http response: {resp}")

    def _process_feature(self, hit_source):
        """
        Format individual features.
        hit_source - the '_source' fields of the elasticsearch response body of a feature.
        """
        starts = hit_source.get('starts')
        stops = hit_source.get('stops')
        contig_ids = hit_source.get('contig_ids')
        strands = hit_source.get('strands')
        if starts != None and len(starts) == len(stops) and \
           len(stops) == len(strands) and len(strands) == len(contig_ids):

            location = [{
                'contig_id': contig_ids[i],
                'start': starts[i],
                'stop': stops[i],
                'strand': strands[i]
            } for i in range(len(starts))]
        else:
            location = []

        if len(location) > 0:
            # TODO: FIX THIS NOT ACTUALLY ACCURATE
            gloc = location[0]
        functions = hit_source.get('functions', [])
        if functions is not None:
            functions = '; '.join(functions)
        return {
            "location": location,
            "feature_id": hit_source.get('id'),
            "dna_sequence": hit_source.get('dna_sequence'),
            "parent_gene": hit_source.get('parent_gene'),
            "size": hit_source.get('size'),
            "functional_descriptions": hit_source.get('functional_descriptions'),
            "warnings": hit_source.get('warnings'),
            "feature_type": hit_source.get('type'),
            "global_location": gloc,
            "aliases": hit_source.get('aliases', {}),
            "function": functions,
            "ontology_terms": {}
        }

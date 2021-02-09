# -*- coding: utf-8 -*-
import json
import uuid
import os
import requests
import time

from MetagenomeAPI.AMAUtils import AMAUtils
from Workspace.WorkspaceClient import Workspace


def get_contig_feature_info(ctx, config, params, sort_by, cache_id, msu, caching):
    """
    Function to get information about contigss
    """
    token = ctx.get('token', None)

    ws = Workspace(config['workspace-url'], token=token)
    ama_utils = AMAUtils(ws)
    params['included_fields'] = ['contig_ids', 'contig_lengths']
    ama = ama_utils.get_annotated_metagenome_assembly(params)['genomes'][0]
    data = ama['data']
    if len(data['contig_ids']) != len(data['contig_lengths']):
        raise RuntimeError(f"contig ids (size: {len(data['contig_ids'])}) and contig "
                           f"lengths (size: {len(data['contig_lengths'])}) sizes do not match.")
    contig_ids = data['contig_ids']
    contig_lengths = data['contig_lengths']
    if msu.status_good:
        feature_counts = msu.search_contig_feature_counts(token,
                                params.get("ref"),
                                min(8000, max(4000, params['start'] + params['limit'])))
                                # len(contig_ids))
        if sort_by[0] == 'contig_id' and sort_by[1] == 0:
            contig_ids, contig_lengths = (list(t) for t in zip(*sorted(zip(contig_ids, contig_lengths), reverse=True)))
        elif sort_by[0] == 'length':
            contig_lengths, contig_ids = (list(t) for t in zip(*sorted(zip(contig_lengths, contig_ids), reverse=sort_by[1] == 0)))
        elif sort_by[0] == 'feature_count':
            # sort the contig_ids  and contig_lengths by feature_counts
            contig_ids, contig_lengths = (list(t) for t in zip(*sorted(zip(contig_ids, contig_lengths), reverse=sort_by[1] == 0, key=lambda x: feature_counts.get(x[0], 0))))
        # get feature_counts
        range_start = params['start']
        range_end = params['start'] + params['limit']

        if range_end > len(contig_ids):
            range_end = len(contig_ids)
        if params['start'] > len(contig_ids):
            contigs = []
        else:
            contigs = [
                {
                    "contig_id": contig_ids[i],
                    "feature_count": feature_counts.get(
                        contig_ids[i],
                        msu.search_contig_feature_count(
                            token,
                            params.get("ref"),
                            contig_ids[i]
                        )
                    ),
                    "length": contig_lengths[i]
                }
                for i in range(range_start, range_end)
            ]
        result =  {
            "contigs": contigs,
            "num_found": len(data['contig_ids']),
            "start": params['start']
        }
        # cache answer for future.
        caching.upload_to_cache(token, cache_id, result)
    else:
        result = {
            "contigs": [],
            "num_found": 0,
            "start": params['start']
        }
    return result


class MetagenomeSearchUtils:
    """Utilities for Searching Annotated Metagenome Assemblies in the KBase Search API"""
    def __init__(self, config):
        if config.get('search-url'):
            self.search_url = config.get('search-url')
        else:
            self.search_url = config.get('kbase-endpoint') + '/searchapi2/rpc'
        # check if server is active.
        resp = requests.get(config.get('kbase-endpoint') + '/searchapi2')
        self.status_good = resp.ok
        self.debug = config.get("debug") == "1"
        self.max_sort_mem_size = 250000

    def search_feature_counts_by_type(self, token, ref):
        """
        Given a reference to a versioned Annotated Metgenome Assembly,
        gets the count of feature types.
        """
        if not self.status_good:
            return {"status": "not_good"}
        num_results = 500000  # adjust this number
        aggs = {
            "group_by_state": {
                "terms": {
                    "field": "type",
                    "size": num_results
                }
            }
        }
        (workspace_id, object_id, version) = ref.split('/')
        # we use namespace 'WSVER' for versioned elasticsearch index.
        ama_id = f'WSVER::{workspace_id}:{object_id}:{version}'

        if token:
            headers = {"Authorization": token}
        else:
            headers = {}
        params = {
            "method": "search_objects",
            "params": {
                "query": {
                    "bool": {
                        "must": [{"term": {"parent_id": ama_id}}]
                    }
                },
                "indexes": ["annotated_metagenome_assembly_features_version"],
                "size": 0,
                "aggs": aggs
            },
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4())
        }
        resp = requests.post(self.search_url, headers=headers, data=json.dumps(params))
        if not resp.ok:
            raise Exception(f"Not able to complete search request against {self.search_url} "
                            f"with parameters: {json.dumps(params)} \nResponse body: {resp.text}")
        respj = resp.json()
        return {
            b['key']: b['count'] for b in respj['result']['aggregations']['group_by_state']['counts']
        }


    def search_contig_feature_count(self, token, ref, contig_id):
        """
        Given a contig, find the number of features it has
        token         - workspace authentication token
        ref           - workspace object reference
        contig_id     - contig id of contig to query around
        """
        extra_must = [{'term': {'contig_ids': contig_id}}]
        # limit and size set to 1 to avoid needlessly unenforcing 'terminate_after' in search_api
        ret = self._elastic_query(token, ref, 1, 1, [('id', 1)], extra_must=extra_must, track_total_hits=True)
        # this will correspond to 'feature_count'
        return ret['num_found']

    def search_contig_feature_counts(self, token, ref, num_results):
        """
        Finds all contig feature counts for a provided AnnotatedMetagneomeAssembly object reference
        token         - workspace authentication token
        ref           - workspace object reference
        num_results   - number of results to return.
        """
        if not self.status_good:
            return {}
        (workspace_id, object_id, version) = ref.split('/')
        # we use namespace 'WSVER' for versioned elasticsearch index.
        ama_id = f'WSVER::{workspace_id}:{object_id}:{version}'

        if token:
            headers = {"Authorization": token}
        else:
            headers = {}
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
                            "size": num_results
                        },
                    }
                }
            },
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4())
        }
        resp = requests.post(self.search_url, headers=headers, data=json.dumps(params))
        if not resp.ok:
            raise Exception(f"Not able to complete search request against {self.search_url} "
                            f"with parameters: {json.dumps(params)} \nResponse body: {resp.text}")
        respj = resp.json()
        return {
            b['key']: b['count'] for b in respj['result']['aggregations']['group_by_state']['counts']
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
        ret = self._elastic_query(token, ref, limit, start, sort_by, extra_must=extra_must, track_total_hits=True)
        if self.debug:
            print(("    (overall-time=" + str(time.time() - t1) + ")"))
        return ret

    def _elastic_query(self, token, ref, limit, start, sort_by, extra_must=[], aggs=None, track_total_hits=False):
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

        if token:
            headers = {"Authorization": token}
        else:
            headers = {}
        query_data = {
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
            },
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4())
        }
        if aggs:
            query_data['params']['aggs'] = aggs
        if track_total_hits:
            query_data['params']['track_total_hits'] = True
        if self.debug:
            print(f"querying {self.search_url}, with params: {json.dumps(query_data)}")
        resp = requests.post(self.search_url, headers=headers, data=json.dumps(query_data))
        if not resp.ok:
            raise Exception(f"Not able to complete search request against {self.search_url} "
                            f"with parameters: {json.dumps(query_data)} \nResponse body: {resp.text}")
        respj = resp.json()
        return self._process_resp(respj['result'], start, query_data)

    def _process_resp(self, resp, start, params):
        """
        Format the response
        resp   - json response from search api
        start  - integer start of pagination
        params - parameters used to search against SearchAPI2
        """
        if resp.get('hits'):
            hits = resp['hits']
            return {
                "num_found": int(resp['count']),
                "start": start,
                "query": params,
                # this should handle empty results list of hits, also sort by feature_id
                "features": [self._process_feature(h['doc']) for h in hits]
            }
        # empty list in reponse for hits
        elif 'hits' in resp:
            return {
                "num_found": int(resp['count']),
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

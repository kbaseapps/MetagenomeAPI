# -*- coding: utf-8 -*-
import json
import os
import requests
import time


class MetagenomeSearchUtils:

    def __init__(self, config):
        if config.get('elastic-url'):
            self.search_url = config.get('search-url')
        else:
            self.search_url = config.get('kbase-endpoint') + '/searchapi2/rpc'

        self.debug = "debug" in config and config["debug"] == "1"
        self.max_sort_mem_size = 250000

    def search_region(self, token, ref, contig_id, region_start, region_length, start, limit, sort_by):
        """
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
            limit = 1000
        if sort_by is None:
            sort_by = [('starts', 1), ('stops', 1)]

        t1 = time.time()
        extra_query = [
            {
                "range": {
                    "starts": {
                        "gte": int(region_start)
                    }
                }
            },{"range": {
                    "stops": {
                        "lte": int(region_start + region_length)
                    }
                }
            }
        ]
        ret = self._elastic_query(token, ref, limit, start, sort_by, extra_query=extra_query)
        # not sure we need to include any of these
        ret['region_start'] = region_start
        ret['contig_id'] = contig_id
        ret['region_length'] = region_length
        if self.debug:
            print(("    (overall-time=" + str(time.time() - t1) + ")"))
        return ret

    def search(self, token, ref, start, limit, sort_by):
        """
        token   - workspace authentication token
        ref     - workspace object reference
        start   - elasticsearch page start delimiter
        limit   - elasticsearch page limit
        sort_by - list of tuples of (field to sort by, ascending bool) for elasticsearch
        """
        if start is None:
            start = 0
        if limit is None:
            limit = 50
        if sort_by is None:
            sort_by = [('id', 1)]

        t1 = time.time()
        ret = self._elastic_query(token, ref, limit, start, sort_by)
        if self.debug:
            print(("    (overall-time=" + str(time.time() - t1) + ")"))
        return ret

    def _elastic_query(self, token, ref, results_size, from_result, sort_by, extra_query=[]):
        """"""
        (workspace_id, object_id, version) = ref.split('/')
        # we use namespace 'WSVER' for versioned elasticsearch index.
        ama_id = f'WSVER::{workspace_id}:{object_id}:{version}'

        headers = {"Authorization": token}
        params = {
            "method": "search_objects",
            "params": {
                "query": {
                    "bool": {"must": [{"term": {"parent_id": ama_id}}] + extra_query}
                    # "term": {"parent_id": ama_id},
                    # **extra_query
                },
                "indexes": ["annotated_metagenome_assembly_features_version:1"],
                "from": from_result,
                "size": results_size,
                "sort": [{s[0]: {"order": "asc" if s[1] else "desc"}} for s in sort_by]
            }
        }
        resp = requests.post(self.search_url, headers=headers, data=json.dumps(params))
        if not resp.ok:
            raise Exception(f"Not able to complete search request against {self.search_url} "
                            f"with parameters: {json.dumps(params)} \nResponse body: {resp.text}")
        respj = resp.json()
        return self._process_resp(respj, from_result, params)

    def _process_resp(self, resp, start, params):
        """"""
        if resp.get('hits') and resp['hits'].get('hits'):
            hits = resp['hits']['hits']
            return {
                "num_found": int(resp['hits']['total']),
                "start": start,
                "query": params,
                "features": [self._process_feature(h['_source']) for h in hits]
            }
        else:
            raise RuntimeError(f"no 'hits' in http response: {resp}")

    def _process_feature(self, hit_source):
        """"""
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
            "feature_type": hit_source.get('type'),
            "global_location": gloc,
            "aliases": {},
            "function": functions,
            "ontology_terms": {}
        }


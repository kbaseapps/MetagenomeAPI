# -*- coding: utf-8 -*-
from ast import keyword
from errno import EILSEQ
import json
import uuid
import requests
import time
import sqlite3
import os
import logging
from gzip import decompress
from multiprocessing import Pool

from MetagenomeAPI.AMAUtils import AMAUtils
from Workspace.WorkspaceClient import Workspace
from installed_clients.AbstractHandleClient import AbstractHandle
from cachetools import TTLCache, cached
from threading import Lock

# 640K should be enough for anyone...
cache = TTLCache(1000, 600)


def get_contig_feature_info(ctx, config, params, sort_by, cache_id, msu, caching):
    """
    Function to get information about contigss
    """
    ws = Workspace(config['workspace-url'], token=ctx['token'])
    ama_utils = AMAUtils(ws)
    params['included_fields'] = ['contig_ids', 'contig_lengths']
    ama = ama_utils.get_annotated_metagenome_assembly(params)['genomes'][0]
    data = ama['data']
    if len(data['contig_ids']) != len(data['contig_lengths']):
        raise RuntimeError(f"contig ids (size: {len(data['contig_ids'])}) and contig "
                           f"lengths (size: {len(data['contig_lengths'])}) sizes do not match.")
    contig_ids = data['contig_ids']
    contig_lengths = data['contig_lengths']
    feature_counts = msu.search_contig_feature_counts(ctx["token"],
                            params.get("ref"),
                            min(8000, max(4000, params['start'] + params['limit'])))
                            # len(contig_ids))
    if sort_by[0] == 'contig_id' and sort_by[1] == 0:
        contig_ids, contig_lengths = (list(t) for t in zip(*sorted(zip(contig_ids, contig_lengths), reverse=True)))
    elif sort_by[0] == 'contig_id':
        contig_ids, contig_lengths = (list(t) for t in zip(*sorted(zip(contig_ids, contig_lengths), reverse=False)))
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
                "feature_count": feature_counts.get(contig_ids[i], 0),
                "length": contig_lengths[i]
            }
            for i in range(range_start, range_end)
        ]
    result =  {
        "contigs": contigs,
        "num_found": len(data['contig_ids']),
        "start": params['start']
    }
    # now cache answer for future.
    caching.upload_to_cache(ctx['token'], cache_id, result)
    return result


class MetagenomeSearchUtils:
    """Utilities for Searching Annotated Metagenome Assemblies in the KBase Search API"""
    def __init__(self, config):
        # check if server is active.
        self.workspace_url = config.get('workspace-url')
        self.handle_service_url = config.get('handle-service-url')
        self.scratch = config.get("scratch", "/tmp")
        self.status_good = True
        self.debug = config.get("debug") == "1"
        if self.debug:
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)
        self.max_sort_mem_size = 250000
        # default fields to use for string searches.
        self.text_fields = ['functions', 'functional_descriptions']
        self.keyword_fields = ["id", "type"]
        if config.get('text-fields'):
            fields = config['text-fields'].split(',')
            # combine with default fields
            self.text_fields = list(set(self.text_fields) + set(fields))
        if config.get('keyword-fields'):
            fields = config['keyword-fields'].split(',')
            # combine with default fields
            self.keyword_fields = list(set(self.keyword_fields) + set(fields))
        self.indexer = Indexer(self.handle_service_url, self.scratch)
        self.pool = Pool(int(config.get("workers", "4")))

    @cached(cache)
    def get_object(self, ref, token):
        ws = Workspace(self.workspace_url, token=token)
        # Let's see if we have access
        # TODO: Cache this lookup
        logging.debug("Doing WS call")
        try:
            incl = ['features_handle_ref']
            obj = ws.get_objects2({'objects': [{'ref': ref, 'included': incl}]})
            objdata = obj["data"][0]
        except Exception as ex:
            logging.error(str(ex)[0:500])
            # Maybe just raise the error
            raise ex
        return objdata

    def search_contig_feature_counts(self, token, ref, num_results):
        """
        Finds all contig feature counts for a provided AnnotatedMetagneomeAssembly object reference
        token         - workspace authentication token
        ref           - workspace object reference
        num_results   - number of results to return.
        """
        conn = self._get_sql_conn(ref, token)
        # If conn is none then it isn't ready yet and return None
        if not conn:
            return {}
        q = "SELECT contig_id, count(*) from features"
        q += " GROUP BY contig_id LIMIT %d" % (num_results)
        resp = {}
        cursor = conn.execute(q)
        for row in cursor:
            resp[row[0]] = row[1]
        return resp

    def search_feature_counts_by_type(self, token, ref):
        # Check permissions
        conn = self._get_sql_conn(ref, token)
        # If conn is none then it isn't ready yet and return None
        if not conn:
            return {}
        q = "SELECT type, count(*) from features GROUP BY type"
        resp = {}
        cursor = conn.execute(q)
        for row in cursor:
            resp[row[0]] = row[1]
        return resp

    def search_contig_feature_count(self, token, ref, contig):
        # Check permissions
        conn = self._get_sql_conn(ref, token)
        # If conn is none then it isn't ready yet and return None
        if not conn:
            return None
        q = "SELECT count(*) from features WHERE contig_id='%s'" % (contig)
        resp = {}
        cursor = conn.execute(q)
        for row in cursor:
            return row[0]

    def search_region(self, token, ref, contig_id, region_start, region_length, start, limit, sort_by):
        conn = self._get_sql_conn(ref, token)
        # If conn is none then it isn't ready yet and return None
        if not conn:
            resp = {
                "num_found": 0,
                "start": start,
                "query": "",
                "region_start": region_start,
                "region_length": region_length,
                "contig_id": contig_id,
                "features": [],
                "indexing": True
            }
            return resp
        stop = region_start + region_length
        q = "SELECT json from features "
        # TODO: Handle direction
        # q += "WHERE ((starts>=%d AND starts<=%d) " % (region_start, stop)
        where_clause = "WHERE ((starts BETWEEN %d AND %d) " % (region_start, stop)
        where_clause += "OR (stops BETWEEN %d AND %d) " % (region_start, stop)
        where_clause += "OR (starts<=%d AND stops>=%d)) " % (region_start, stop)
        where_clause += " AND contig_id='%s'" % (contig_id)

        cursor = conn.execute("SELECT count(*) from features " + where_clause)
        ct = cursor.fetchone()[0]

        q += where_clause
        q += self._order_by(sort_by)
        q += " LIMIT %d OFFSET %d" % (limit, start)
        query = q
        cursor = conn.execute(query)
        features = []
        for row in cursor:
            f = self._process_features(json.loads(row[0]))
            features.append(f)

        resp = {
            "num_found": ct,
            "start": start,
            "query": query,
            "region_start": region_start,
            "region_length": region_length,
            "contig_id": contig_id,
            "features": features
        }
        return resp

    def search(self, token, ref, start, limit, sort_by, query):
        """
        Search features against one Annotated Metagenome Assembly, with or without a query string.
        token   - workspace authentication token
        ref     - workspace object reference
        start   - elasticsearch page start delimiter
        limit   - elasticsearch page limit
        sort_by - list of tuples of (field to sort by, ascending bool) for elasticsearch
        query   - text to search on fields listed in 'text_fields' and 'keyword_fields' env vars.
        """
        if start is None:
            start = 0
        if limit is None:
            limit = 50
        if sort_by is None:
            sort_by = [('id', 1)]
        conn = self._get_sql_conn(ref, token)
        # If conn is none then it isn't ready yet and return None
        if not conn:
            resp = {
                "num_found": 0,
                "start": start,
                "query": "",
                "features": [],
                "indexing": True
            }
            return resp
        q = "SELECT json from features "

        # Handle query
        where_clause = ""
        if query is not None and query != "":
            ele = []
            for tok in str(query).split():
                for k in self.keyword_fields:
                    ele.append("(%s='%s')" % (k, tok))
                for k in self.text_fields:
                    ele.append("(%s like '%%%s%%')" % (k, tok))
            if len(ele) > 0:
                where_clause = "WHERE (%s) " % (" OR ".join(ele))

        # Get Count
        cursor = conn.execute("SELECT count(*) from features " + where_clause)
        ct = cursor.fetchone()[0]

        q += where_clause
        q += self._order_by(sort_by)
        q += " LIMIT %d OFFSET %d" % (limit, start)

        query = q
        cursor = conn.execute(query)
        features = []
        for row in cursor:
            f = self._process_features(json.loads(row[0]))
            features.append(f)

        resp = {
            "num_found": ct,
            "start": start,
            "query": query,
            "features": features
        }
        return resp

    def _order_by(self, sort_by):
        # handle sort_by
        if sort_by:
            ele = []
            for sb in sort_by:
                key = sb[0]
                if key == "contig_ids":
                    key = "contig_id"
                if sb[1] > 0:
                    ele.append("%s ASC" % (key))
                else:
                    ele.append("%s DESC" % (key))
            if len(ele) > 0:
                return " ORDER BY %s" % (','.join(ele))
        return ""

    def _process_features(self, feature):
        locs = []
        for loc in feature["location"]:
            locs.append({
                "contig_id": loc[0],
                "start": loc[1],
                "stop": loc[3],
                "strand": loc[2]
            })
        return {
            "aliases": feature.get("aliases"),
            "feature_id": feature["id"],
            "feature_type": feature["type"],
            "location": locs,
            "global_location": locs[0],
            "dna_sequence": feature["dna_sequence"],
            "parent_gene": feature.get("parent_gene"),
            "size": feature["dna_sequence_length"],
            "function": feature.get("functions", [""])[0],
            "functional_descriptions": None,
            "ontology_terms": {},
            "warnings": feature.get("warnings")
        }

    def _get_sql_conn(self, ref, token):
        """
        This will connect to an existing sql database.
        If one doesn't exist then it request it be generate it.
        """
        objdata = self.get_object(ref, token)
        if objdata is None:
            return None

        ready, sqlf = self.indexer.is_indexed(ref)
        if ready:
            conn = sqlite3.connect(sqlf)
            return conn
        args = [objdata, sqlf, token]
        self.pool.apply_async(self.indexer._create_index, args, {}, None, self._error)
        return None
        # return self._create_index(objdata, sqlf, token)

    def _error(self, err):
        logging.error(err)
        print(err)

class Indexer():
    # Allow up to 15 minutes to index
    _TIMEOUT = 15*60
    _LOG_TIME = 30

    def __init__(self, handle_service_url, scratch):
        self.handle_service_url = handle_service_url
        self.scratch = scratch

    def is_indexed(self, ref):
        sqlf = self.sqlfile(ref)
        return os.path.exists(sqlf), sqlf

    def sqlfile(self, ref):
        return "%s/%s.sql" % (self.scratch, ref.replace("/", ":"))

    def _create_index(self, objdata, sqlf, token):
        """
        This actually creates the sqlite file and
        populates it.

        Inputs:
        obj: previously fetched WS object data
        sqlf: name of the sqlite file to generate
        token: token associated with the request
        """
        if os.path.exists(sqlf):
            logging.info("Index already generated")
            return
        copied = objdata.get("copied")

        # If this is a copy, let's try to reuse the
        # the source sql
        if copied:
            osql = self.sqlfile(copied)
            if os.path.exists(osql):
                os.link(osql, sqlf)
                return

        tmpsql = '%s.tmp' % (sqlf)
        if os.path.exists(tmpsql):
            st = os.stat(tmpsql)
            if st.st_mtime > (time.time() - self._TIMEOUT):
                logging.debug("Already indexing")
                return
            # Potentially failed attempt, cleanup old file
            logging.warn("Removing stale index file")
            os.unlink(tmpsql)

        logging.info("Generating index %s" % (sqlf))
        start_time = time.time()
        conn = sqlite3.connect(tmpsql)
        hid = objdata["data"]["features_handle_ref"]
        logging.debug("Fetching hid=%s" % (hid))
        features = self._fetch_data(hid, token)

        # TODO: Make these more configurable
        conn.execute('''CREATE TABLE features
             (id INT PRIMARY KEY     NOT NULL,
             contig_id        TEXT    NOT NULL,
             type             TEXT    NOT NULL,
             starts           INT     NOT NULL,
             stops            INT     NOT NULL,
             size             INT     NOT NULL,
             functions        TEXT    NOT NULL,
             functional_descriptions   TEXT    NOT NULL,
             strands          TEXT    NOT NULL,
             json             BLOB    NOT NULL);
             ''')
        conn.execute("create index starts on features (starts);")
        conn.execute("create index stops on features (stops);")
        conn.execute("create index cid on features (contig_id);")
        conn.execute("create index size on features (size);")
        conn.execute("create index type on features (type);")
        conn.execute("create index functions on features (functions);")
        conn.execute("create index descr on features (functional_descriptions);")
        ct = 0
        seen = {}
        last_update = time.time()
        logging.debug("Starting indexing: %s" % (sqlf))
        for f in features:
            id = f["id"]
            if id in seen:
                logging.warn("Duplicate ID")
                continue
            seen[id] = 1
            loc = f["location"][0]
            length = loc[3]
            if loc[2] == "+":
                start = loc[1]
                stop = start + length
            else:
                start = loc[1] - length
                stop = loc[1]

            values = [f["id"],
                      loc[0],
                      f["type"],
                      start,
                      stop,
                      length,
                      f.get("functions",[''])[0],
                      f.get("functions",[''])[0],
                      loc[2],
                      json.dumps(f)]
#                      f.get("functions",[''])[0].replace('""', "''"),
    
            query = "INSERT INTO features VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            conn.execute(query, values)
            elapsed = time.time() - last_update
            if elapsed >= self._LOG_TIME:
                logging.info("Indexed %s %d features" % (sqlf, ct))
                conn.commit()
                last_update = time.time()
            ct += 1
        conn.commit()
        os.rename(tmpsql, sqlf)
        # If this was a copy then let's link them together
        if copied:
            os.link(sqlf, osql)
        logging.info("Indexing complete in %ds" % (time.time() - start_time))
        return True

    def _fetch_data(self, hid, token):
        hs = AbstractHandle(self.handle_service_url, token=token)
        hobj = hs.hids_to_handles([hid])[0]
        url = "%s/node/%s?download_raw" % (hobj["url"], hobj["id"])
        resp = requests.get(url, headers={"Authorization": "OAuth %s" % (token)})
        rawdata = decompress(resp.content)
        features = json.loads(rawdata)
        return features


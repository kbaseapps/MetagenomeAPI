# -*- coding: utf-8 -*-
import os
import base64
import time
import subprocess
import traceback
import string
import tempfile

from Workspace.WorkspaceClient import Workspace as Workspace
from MetagenomeAPI.CombinedLineIterator import CombinedLineIterator

class BinnedContigsIndexer:

    def __init__(self, config):
        self.binnedcontigs_column_props_map = {
            "bin_id": {"col": 1, "type": ""},
            "n_contigs": {"col": 2, "type": "n"},
            "sum_contig_len": {"col": 3, "type": "n"},
            "gc": {"col": 4, "type": "n"},
            "cov": {"col": 5, "type": "n"}
        }
        self.contigs_in_bin_column_props_map = {
            "id": {"col": 1, "type": ""},
            "len": {"col": 2, "type": "n"},
            "gc": {"col": 3, "type": "n"},
            "cov": {"col": 4, "type": "n"}
        }

        self.BIN_SUFFIX = '_bins'
        self.CONTIGS_SUFFIX = '_ctgs'

        self.ws_url = config["workspace-url"]
        self.metagenome_index_dir = config["metagenome-index-dir"]
        if not os.path.isdir(self.metagenome_index_dir):
            os.makedirs(self.metagenome_index_dir)
        self.debug = "debug" in config and config["debug"] == "1"
        self.max_sort_mem_size = 250000
        self.unicode_comma = u"\uFF0C"

    def search_binned_contigs(self, token, ref, query, sort_by, start, limit, num_found):
        if query is None:
            query = ""
        if start is None:
            start = 0
        if limit is None:
            limit = 50
        if self.debug:
            print("Search: BinnedContigs=" + ref + ", query=[" + query + "], sort-by=[" +
                  self.get_sorting_code(self.binnedcontigs_column_props_map, sort_by) +
                  "], start=" + str(start) + ", limit=" + str(limit))
            t1 = time.time()
        inner_chsum = self.check_binnedcontigs_cache(ref, token)
        index_iter = self.get_binnedcontigs_sorted_iterator(inner_chsum, sort_by)
        ret = self.filter_binnedcontigs_query(index_iter, query, start, limit, num_found)
        if self.debug:
            print("    (overall-time=" + str(time.time() - t1) + ")")
        return ret

    def to_text(self, mapping, key):
        if key not in mapping or mapping[key] is None:
            return ""
        value = mapping[key]
        if type(value) is list:
            return ",".join(str(x) for x in value if x)
        return str(value)

    def save_binnedcontigs_tsv(self, bins, inner_chsum):
        outfile = tempfile.NamedTemporaryFile(dir=self.metagenome_index_dir,
                                              prefix=inner_chsum + self.BIN_SUFFIX,
                                              suffix=".tsv", delete=False)
        with outfile:
            for bin_data in bins:
                bin_id = self.to_text(bin_data, "bid")
                bin_n_contigs = ''
                if 'n_contigs' in bin_data:
                    bin_n_contigs = str(bin_data['n_contigs'])
                bin_sum_contig_len = ''
                if 'sum_contig_len' in bin_data:
                    bin_sum_contig_len = str(bin_data['sum_contig_len'])
                bin_gc = ''
                if 'gc' in bin_data:
                    bin_gc = str(bin_data['gc'])
                bin_cov = ''
                if 'cov' in bin_data:
                    bin_cov = str(bin_data['cov'])

                line = u"\t".join(x for x in [bin_id, bin_n_contigs, bin_sum_contig_len, bin_gc, bin_cov]) + u"\n"
                outfile.write(line.encode("utf-8"))

        subprocess.Popen(["gzip", outfile.name],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE).wait()
        os.rename(outfile.name + ".gz", os.path.join(self.metagenome_index_dir,
                                                     inner_chsum + self.BIN_SUFFIX + ".tsv.gz"))

    def check_binnedcontigs_cache(self, ref, token):
        ws = Workspace(self.ws_url, token=token)
        info = ws.get_object_info3({"objects": [{"ref": ref}]})['infos'][0]
        inner_chsum = info[8]
        index_file = os.path.join(self.metagenome_index_dir, inner_chsum + self.BIN_SUFFIX + ".tsv.gz")
        if not os.path.isfile(index_file):
            if self.debug:
                print("    Loading WS object...")
                t1 = time.time()

            included = ["/bins/[*]/bid",
                        "/bins/[*]/gc",
                        "/bins/[*]/n_contigs",
                        "/bins/[*]/sum_contig_len",
                        "/bins/[*]/cov"
                        ]

            binnedcontigs = ws.get_objects2({'objects': [{'ref': ref, 'included': included}]})['data'][0]['data']
            self.save_binnedcontigs_tsv(binnedcontigs["bins"], inner_chsum)
            if self.debug:
                print("    (time=" + str(time.time() - t1) + ")")
        return inner_chsum

    def get_column_props(self, column_props_map, col_name):
        if col_name not in column_props_map:
            raise ValueError("Unknown column name '" + col_name + "', " +
                             "please use one of " + str(column_props_map.keys()))
        return column_props_map[col_name]

    def get_sorting_code(self, column_props_map, sort_by):
        ret = ""
        if sort_by is None or len(sort_by) == 0:
            return ret
        for column_sorting in sort_by:
            col_name = column_sorting[0]
            col_props = self.get_column_props(column_props_map, col_name)
            col_pos = str(col_props["col"])
            ascending_order = column_sorting[1]
            ret += col_pos + ('a' if ascending_order else 'd')
        return ret

    def get_binnedcontigs_sorted_iterator(self, inner_chsum, sort_by):
        return self.get_sorted_iterator(inner_chsum, sort_by, self.BIN_SUFFIX,
                                        self.binnedcontigs_column_props_map)

    def get_sorted_iterator(self, inner_chsum, sort_by, item_type, column_props_map):
        input_file = os.path.join(self.metagenome_index_dir, inner_chsum + item_type + ".tsv.gz")
        if not os.path.isfile(input_file):
            raise ValueError("File not found: " + input_file)
        if sort_by is None or len(sort_by) == 0:
            return CombinedLineIterator(input_file)
        cmd = "gunzip -c \"" + input_file + "\" | sort -f -t\\\t"
        for column_sorting in sort_by:
            col_name = column_sorting[0]
            col_props = self.get_column_props(column_props_map, col_name)
            col_pos = str(col_props["col"])
            ascending_order = column_sorting[1]
            sort_arg = "-k" + col_pos + "," + col_pos + col_props["type"]
            if not ascending_order:
                sort_arg += "r"
            cmd += " " + sort_arg
        fname = (inner_chsum + "_" + item_type + "_" +
                 self.get_sorting_code(column_props_map, sort_by))
        final_output_file = os.path.join(self.metagenome_index_dir, fname + ".tsv.gz")
        if not os.path.isfile(final_output_file):
            if self.debug:
                print("    Sorting...")
                t1 = time.time()
            need_to_save = os.path.getsize(input_file) > self.max_sort_mem_size
            if need_to_save:
                outfile = tempfile.NamedTemporaryFile(dir=self.metagenome_index_dir,
                                                      prefix=fname + "_", suffix=".tsv.gz", delete=False)
                outfile.close()
                output_file = outfile.name
                cmd += " | gzip -c > \"" + output_file + "\""
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            if not need_to_save:
                if self.debug:
                    print("    (time=" + str(time.time() - t1) + ")")
                return CombinedLineIterator(p)
            else:
                p.wait()
                os.rename(output_file, final_output_file)
                if self.debug:
                    print("    (time=" + str(time.time() - t1) + ")")
        return CombinedLineIterator(final_output_file)

    def filter_binnedcontigs_query(self, index_iter, query, start, limit, num_found):
        query_words = (str(query).lower().replace('\n',' ').replace('\r',' ').replace('\t',' ').replace(',',' ')).split()
        # query_words = str(query).lower().translate(string.maketrans("\r\n\t,", "    ")).split()
        if self.debug:
            print("    Filtering...")
            t1 = time.time()
        fcount = 0
        bins = []
        with index_iter:
            for line in index_iter:
                if all(word in line.lower() for word in query_words):
                    if fcount >= start and fcount < start + limit:
                        bins.append(self.unpack_bin(line.rstrip('\n')))
                    fcount += 1
                    if num_found is not None and fcount >= start + limit:
                        # Having shortcut when real num_found was already known
                        fcount = num_found
                        break
        if self.debug:
                print("    (time=" + str(time.time() - t1) + ")")
        return {"num_found": fcount, "start": start, "bins": bins,
                "query": query}

    def unpack_bin(self, line, items=None):
        try:
            if items is None:
                items = line.split('\t')

            bin_id = items[0]
            bin_n_contigs = None
            bin_sum_contig_len = None
            bin_gc = None
            bin_cov = None

            if items[1]:
                bin_n_contigs = int(items[1])
            if items[2]:
                bin_sum_contig_len = int(items[2])
            if items[3]:
                bin_gc = float(items[3])
            if items[4]:
                bin_cov = float(items[4])

            return {'bin_id': bin_id,
                    'n_contigs': bin_n_contigs,
                    'sum_contig_len': bin_sum_contig_len,
                    'gc': bin_gc,
                    'cov': bin_cov
                    }
        except:
            raise ValueError("Error parsing bin from: [" + line + "]\n" +
                             "Cause: " + traceback.format_exc())

    def search_contigs_in_bin(self, token, ref, bin_id, query, sort_by, start, limit, num_found):
        if bin_id is None:
            raise ValueError('bin_id input parameter field is missing')
        if query is None:
            query = ""
        if start is None:
            start = 0
        if limit is None:
            limit = 50
        if self.debug:
            print("Search contigs in bin: BinnedContigs=" + ref + ", bin=" + bin_id + ", query=[" + query + "], " +
                  "sort-by=[" + self.get_sorting_code(self.contigs_in_bin_column_props_map,
                  sort_by) + "], start=" + str(start) + ", limit=" + str(limit))
            t1 = time.time()
        inner_chsum = self.check_contigs_in_bin_cache(ref, bin_id, token)
        index_iter = self.get_contigs_in_bin_sorted_iterator(inner_chsum, sort_by)
        ret = self.filter_contig_query(index_iter, query, bin_id, start, limit, num_found)
        if self.debug:
            print("    (overall-time=" + str(time.time() - t1) + ")")
        return ret

    def check_contigs_in_bin_cache(self, binnedcontigs_ref, bin_id, token):
        ws = Workspace(self.ws_url, token=token)

        info = ws.get_object_info3({"objects": [{"ref": binnedcontigs_ref}]})['infos'][0]

        # base64 encode the string so it is safe for filenames and still unique per contig id
        b64key = base64.urlsafe_b64encode(bin_id.encode("utf-8")).decode('utf-8')

        inner_chsum = info[8] + '_' + b64key
        index_file = os.path.join(self.metagenome_index_dir, inner_chsum + self.CONTIGS_SUFFIX + ".tsv.gz")
        if not os.path.isfile(index_file):
            if self.debug:
                t1 = time.time()

            # get the position in the array of the bin
            binnedcontigs = ws.get_objects2({'objects': [{'ref': binnedcontigs_ref,
                                             'included': ["/bins/[*]/bid"]}]})['data'][0]['data']
            pos = 0
            found = False
            for b in binnedcontigs['bins']:
                if b['bid'] == bin_id:
                    found = True
                    break
                pos += 1
            if not found:
                raise ValueError('No Bin with ID: "' + bin_id + '" found.')

            # get the contigs map
            selection = ws.get_objects2({'objects': [{'ref': binnedcontigs_ref,
                                         'included': ['/bins/' + str(pos) + '/contigs',
                                                      '/bins/' + str(pos) + '/bid']}]})['data'][0]['data']
            if selection['bins'][0]['bid'] != bin_id:
                raise ValueError('Something went wrong- bin ids do not match on second ws get_objects2 call')

            contigs = selection['bins'][0]['contigs']
            self.save_contigs_in_bin_tsv(contigs, inner_chsum)
            if self.debug:
                print("    (time=" + str(time.time() - t1) + ")")
        return inner_chsum

    def save_contigs_in_bin_tsv(self, contigs, inner_chsum):
        outfile = tempfile.NamedTemporaryFile(dir=self.metagenome_index_dir, mode='w+',
                                              prefix=inner_chsum + self.CONTIGS_SUFFIX, suffix=".tsv", delete=False)

        with outfile:
            for contig_id in contigs:
                info = contigs[contig_id]

                contig_len = ''
                if 'len' in info:
                    contig_len = str(info['len'])
                contig_gc = ''
                if 'gc' in info:
                    contig_gc = str(info['gc'])
                contig_cov = ''
                if 'cov' in info:
                    contig_cov = str(info['cov'])

                outfile.write("\t".join([contig_id, contig_len, contig_gc, contig_cov]) + "\n")

        subprocess.Popen(["gzip", outfile.name],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE).wait()
        os.rename(outfile.name + ".gz",
                  os.path.join(self.metagenome_index_dir, inner_chsum + self.CONTIGS_SUFFIX + ".tsv.gz"))

    def get_contigs_in_bin_sorted_iterator(self, inner_chsum, sort_by):
        return self.get_sorted_iterator(inner_chsum, sort_by, self.CONTIGS_SUFFIX,
                                        self.contigs_in_bin_column_props_map)

    def filter_contig_query(self, index_iter, query, bin_id, start, limit, num_found):
        query_words = (str(query).lower().replace('\n',' ').replace('\r',' ').replace('\t',' ').replace(',',' ')).split()
        # query_words = str(query).lower().translate(string.maketrans("\r\n\t,", "    ")).split()
        if self.debug:
            print("    Filtering...")
            t1 = time.time()
        fcount = 0
        contigs = []
        with index_iter:
            for line in index_iter:
                if all(word in line.lower() for word in query_words):
                    if fcount >= start and fcount < start + limit:
                        contigs.append(self.unpack_contig_in_bin(line.rstrip('\n')))
                    fcount += 1
                    if num_found is not None and fcount >= start + limit:
                        # Having shortcut when real num_found was already known
                        fcount = num_found
                        break
        if self.debug:
                print("    (time=" + str(time.time() - t1) + ")")
        return {"num_found": fcount, "start": start, "contigs": contigs,
                "query": query, 'bin_id': bin_id}

    def unpack_contig_in_bin(self, line, items=None):
        try:
            if items is None:
                items = line.split('\t')
            contig_id = items[0]
            contig_len = int(items[1])
            contig_gc = float(items[2])
            contig_cov = None
            if items[3]:
                contig_cov = float(items[3])
            return {'contig_id': contig_id,
                    'len': contig_len,
                    'gc': contig_gc,
                    'cov': contig_cov}
        except:
            raise ValueError("Error parsing contig from: [" + line + "]\n" +
                             "Cause: " + traceback.format_exc())

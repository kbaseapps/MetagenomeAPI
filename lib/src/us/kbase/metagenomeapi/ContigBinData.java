
package us.kbase.metagenomeapi;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * <p>Original spec-file type: ContigBinData</p>
 * <pre>
 * bin_id          - id of the bin
 * n_contigs       - number of contigs in this bin
 * gc              - GC content over all the contigs
 * sum_contig_len  - (bp) total length of the contigs
 * cov             - coverage over the bin (if available, may be null)
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "bin_id",
    "n_contigs",
    "gc",
    "sum_contig_len",
    "cov"
})
public class ContigBinData {

    @JsonProperty("bin_id")
    private String binId;
    @JsonProperty("n_contigs")
    private Long nContigs;
    @JsonProperty("gc")
    private Double gc;
    @JsonProperty("sum_contig_len")
    private Long sumContigLen;
    @JsonProperty("cov")
    private Double cov;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("bin_id")
    public String getBinId() {
        return binId;
    }

    @JsonProperty("bin_id")
    public void setBinId(String binId) {
        this.binId = binId;
    }

    public ContigBinData withBinId(String binId) {
        this.binId = binId;
        return this;
    }

    @JsonProperty("n_contigs")
    public Long getNContigs() {
        return nContigs;
    }

    @JsonProperty("n_contigs")
    public void setNContigs(Long nContigs) {
        this.nContigs = nContigs;
    }

    public ContigBinData withNContigs(Long nContigs) {
        this.nContigs = nContigs;
        return this;
    }

    @JsonProperty("gc")
    public Double getGc() {
        return gc;
    }

    @JsonProperty("gc")
    public void setGc(Double gc) {
        this.gc = gc;
    }

    public ContigBinData withGc(Double gc) {
        this.gc = gc;
        return this;
    }

    @JsonProperty("sum_contig_len")
    public Long getSumContigLen() {
        return sumContigLen;
    }

    @JsonProperty("sum_contig_len")
    public void setSumContigLen(Long sumContigLen) {
        this.sumContigLen = sumContigLen;
    }

    public ContigBinData withSumContigLen(Long sumContigLen) {
        this.sumContigLen = sumContigLen;
        return this;
    }

    @JsonProperty("cov")
    public Double getCov() {
        return cov;
    }

    @JsonProperty("cov")
    public void setCov(Double cov) {
        this.cov = cov;
    }

    public ContigBinData withCov(Double cov) {
        this.cov = cov;
        return this;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        return ((((((((((((("ContigBinData"+" [binId=")+ binId)+", nContigs=")+ nContigs)+", gc=")+ gc)+", sumContigLen=")+ sumContigLen)+", cov=")+ cov)+", additionalProperties=")+ additionalProperties)+"]");
    }

}

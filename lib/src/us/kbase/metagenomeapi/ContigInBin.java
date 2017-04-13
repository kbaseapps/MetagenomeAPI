
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
 * <p>Original spec-file type: ContigInBin</p>
 * <pre>
 * contig_id       - id of the contig
 * len             - (bp) length of the contig
 * gc              - GC content over the contig
 * cov             - coverage over the contig (if available, may be null)
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "contig_id",
    "len",
    "gc",
    "cov"
})
public class ContigInBin {

    @JsonProperty("contig_id")
    private String contigId;
    @JsonProperty("len")
    private Long len;
    @JsonProperty("gc")
    private Double gc;
    @JsonProperty("cov")
    private Double cov;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("contig_id")
    public String getContigId() {
        return contigId;
    }

    @JsonProperty("contig_id")
    public void setContigId(String contigId) {
        this.contigId = contigId;
    }

    public ContigInBin withContigId(String contigId) {
        this.contigId = contigId;
        return this;
    }

    @JsonProperty("len")
    public Long getLen() {
        return len;
    }

    @JsonProperty("len")
    public void setLen(Long len) {
        this.len = len;
    }

    public ContigInBin withLen(Long len) {
        this.len = len;
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

    public ContigInBin withGc(Double gc) {
        this.gc = gc;
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

    public ContigInBin withCov(Double cov) {
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
        return ((((((((((("ContigInBin"+" [contigId=")+ contigId)+", len=")+ len)+", gc=")+ gc)+", cov=")+ cov)+", additionalProperties=")+ additionalProperties)+"]");
    }

}

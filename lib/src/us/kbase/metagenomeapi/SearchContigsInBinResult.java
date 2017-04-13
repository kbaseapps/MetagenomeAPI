
package us.kbase.metagenomeapi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * <p>Original spec-file type: SearchContigsInBinResult</p>
 * <pre>
 * num_found - number of all items found in query search (with 
 *     only part of it returned in "bins" list).
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "query",
    "bin_id",
    "start",
    "contigs",
    "num_found"
})
public class SearchContigsInBinResult {

    @JsonProperty("query")
    private String query;
    @JsonProperty("bin_id")
    private String binId;
    @JsonProperty("start")
    private Long start;
    @JsonProperty("contigs")
    private List<ContigInBin> contigs;
    @JsonProperty("num_found")
    private Long numFound;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("query")
    public String getQuery() {
        return query;
    }

    @JsonProperty("query")
    public void setQuery(String query) {
        this.query = query;
    }

    public SearchContigsInBinResult withQuery(String query) {
        this.query = query;
        return this;
    }

    @JsonProperty("bin_id")
    public String getBinId() {
        return binId;
    }

    @JsonProperty("bin_id")
    public void setBinId(String binId) {
        this.binId = binId;
    }

    public SearchContigsInBinResult withBinId(String binId) {
        this.binId = binId;
        return this;
    }

    @JsonProperty("start")
    public Long getStart() {
        return start;
    }

    @JsonProperty("start")
    public void setStart(Long start) {
        this.start = start;
    }

    public SearchContigsInBinResult withStart(Long start) {
        this.start = start;
        return this;
    }

    @JsonProperty("contigs")
    public List<ContigInBin> getContigs() {
        return contigs;
    }

    @JsonProperty("contigs")
    public void setContigs(List<ContigInBin> contigs) {
        this.contigs = contigs;
    }

    public SearchContigsInBinResult withContigs(List<ContigInBin> contigs) {
        this.contigs = contigs;
        return this;
    }

    @JsonProperty("num_found")
    public Long getNumFound() {
        return numFound;
    }

    @JsonProperty("num_found")
    public void setNumFound(Long numFound) {
        this.numFound = numFound;
    }

    public SearchContigsInBinResult withNumFound(Long numFound) {
        this.numFound = numFound;
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
        return ((((((((((((("SearchContigsInBinResult"+" [query=")+ query)+", binId=")+ binId)+", start=")+ start)+", contigs=")+ contigs)+", numFound=")+ numFound)+", additionalProperties=")+ additionalProperties)+"]");
    }

}


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
import us.kbase.common.service.UObject;


/**
 * <p>Original spec-file type: getAnnotatedMetagenomeAssemblyOutput</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "genomes"
})
public class GetAnnotatedMetagenomeAssemblyOutput {

    @JsonProperty("genomes")
    private List<UObject> genomes;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("genomes")
    public List<UObject> getGenomes() {
        return genomes;
    }

    @JsonProperty("genomes")
    public void setGenomes(List<UObject> genomes) {
        this.genomes = genomes;
    }

    public GetAnnotatedMetagenomeAssemblyOutput withGenomes(List<UObject> genomes) {
        this.genomes = genomes;
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
        return ((((("GetAnnotatedMetagenomeAssemblyOutput"+" [genomes=")+ genomes)+", additionalProperties=")+ additionalProperties)+"]");
    }

}

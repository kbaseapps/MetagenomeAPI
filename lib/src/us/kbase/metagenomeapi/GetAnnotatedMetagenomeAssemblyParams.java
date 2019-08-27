
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
 * <p>Original spec-file type: getAnnotatedMetagenomeAssemblyParams</p>
 * <pre>
 * ref - workspace reference to AnnotatedMetagenomeAssembly Object
 * included_fields - The fields to include from the Object
 * included_feature_fields -
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "ref",
    "included_fields"
})
public class GetAnnotatedMetagenomeAssemblyParams {

    @JsonProperty("ref")
    private java.lang.String ref;
    @JsonProperty("included_fields")
    private List<String> includedFields;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("ref")
    public java.lang.String getRef() {
        return ref;
    }

    @JsonProperty("ref")
    public void setRef(java.lang.String ref) {
        this.ref = ref;
    }

    public GetAnnotatedMetagenomeAssemblyParams withRef(java.lang.String ref) {
        this.ref = ref;
        return this;
    }

    @JsonProperty("included_fields")
    public List<String> getIncludedFields() {
        return includedFields;
    }

    @JsonProperty("included_fields")
    public void setIncludedFields(List<String> includedFields) {
        this.includedFields = includedFields;
    }

    public GetAnnotatedMetagenomeAssemblyParams withIncludedFields(List<String> includedFields) {
        this.includedFields = includedFields;
        return this;
    }

    @JsonAnyGetter
    public Map<java.lang.String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(java.lang.String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public java.lang.String toString() {
        return ((((((("GetAnnotatedMetagenomeAssemblyParams"+" [ref=")+ ref)+", includedFields=")+ includedFields)+", additionalProperties=")+ additionalProperties)+"]");
    }

}

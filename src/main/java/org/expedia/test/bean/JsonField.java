package org.expedia.test.bean;

import java.io.Serializable;

import org.codehaus.jackson.annotate.JsonProperty;

public class JsonField implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -1170978387066703409L;

    private char fieldDelimiter = '\001';

    public JsonField() {
    }

    @JsonProperty("site_name")
    private String currSite;

    @JsonProperty("local_date")
    private String currDate;

    @JsonProperty("data")
    private String data;

    public String getCurrSite() {
        return currSite;
    }

    public void setFieldDelimiter(char fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    // getter and setter methods

    public String getCurrDate() {
        return currDate;
    }

    public String getData() {
        return data;
    }

    public void setCurrSite(String currSite) {
        this.currSite = currSite;
    }

    public void setCurrDate(String currDate) {
        this.currDate = currDate;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return new StringBuilder(currSite).append(fieldDelimiter).append(currDate).append(fieldDelimiter).append(data).toString();
    }
}

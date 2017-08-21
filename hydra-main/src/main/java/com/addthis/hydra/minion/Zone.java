package com.addthis.hydra.minion;


import com.addthis.codec.codables.Codable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;

@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Zone implements Codable, AutoCloseable {
    private String zoneID;
    private String rackID;
    private String machineID;

    @JsonCreator Zone(@JsonProperty(value = "zoneID", required = true) String zoneID,
                      @JsonProperty(value = "rackID", required = true) String rackID,
                      @JsonProperty(value = "machineID", required = true) String machineID) {
        this.zoneID = zoneID;
        this.rackID = rackID;
        this.machineID = machineID;
    }

    public String getRack() {
        return rackID;
    }

    public void setRack(String rack) {
        this.rackID = rack;
    }

    public String getZone() {
        return zoneID;
    }

    public void setZone(String zone) {
        this.zoneID = zone;
    }

    public String getMachine() {
        return machineID;
    }

    public void setMachine(String machine) {
        this.machineID = machine;
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Zone zone = (Zone) obj;
        return zone.getZone().equals(this.zoneID) && zone.getRack().equals(this.rackID) && zone.getMachine().equals(this.machineID);
    }

    @Override
    public String toString() {
        return "Zone: " + getZone() + " Rack: " + getRack() + " Machine: " + getMachine();
    }

    @Override
    public void close() throws Exception {

    }
}

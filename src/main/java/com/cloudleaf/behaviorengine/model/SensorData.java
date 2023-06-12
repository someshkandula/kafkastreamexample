package com.cloudleaf.behaviorengine.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SensorData {
    private String evt_time;
    private String s_id;
    private String c_id;
    private String value;
    private String leaf_id;
    private String peripheral_id;
}

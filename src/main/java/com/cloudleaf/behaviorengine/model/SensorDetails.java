package com.cloudleaf.behaviorengine.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SensorDetails {
  private String sensorId;
  private String command;
  private String createDate;
  private String cloudDate;
  private String vendor;
  private SensorData sensorData;
}

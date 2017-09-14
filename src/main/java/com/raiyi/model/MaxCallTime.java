/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.raiyi.model;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class MaxCallTime extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"MaxCallTime\",\"namespace\":\"com.raiyi.model\",\"fields\":[{\"name\":\"tele_max_call_time\",\"type\":\"int\",\"doc\":\"10000最大通话时长\",\"default\":-1},{\"name\":\"mit_max_call_time\",\"type\":\"int\",\"doc\":\"12300最大通话时长\",\"default\":-1}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  /** 10000最大通话时长 */
  @Deprecated public int tele_max_call_time;
  /** 12300最大通话时长 */
  @Deprecated public int mit_max_call_time;

  /**
   * Default constructor.
   */
  public MaxCallTime() {}

  /**
   * All-args constructor.
   */
  public MaxCallTime(java.lang.Integer tele_max_call_time, java.lang.Integer mit_max_call_time) {
    this.tele_max_call_time = tele_max_call_time;
    this.mit_max_call_time = mit_max_call_time;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return tele_max_call_time;
    case 1: return mit_max_call_time;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: tele_max_call_time = (java.lang.Integer)value$; break;
    case 1: mit_max_call_time = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'tele_max_call_time' field.
   * 10000最大通话时长   */
  public java.lang.Integer getTeleMaxCallTime() {
    return tele_max_call_time;
  }

  /**
   * Sets the value of the 'tele_max_call_time' field.
   * 10000最大通话时长   * @param value the value to set.
   */
  public void setTeleMaxCallTime(java.lang.Integer value) {
    this.tele_max_call_time = value;
  }

  /**
   * Gets the value of the 'mit_max_call_time' field.
   * 12300最大通话时长   */
  public java.lang.Integer getMitMaxCallTime() {
    return mit_max_call_time;
  }

  /**
   * Sets the value of the 'mit_max_call_time' field.
   * 12300最大通话时长   * @param value the value to set.
   */
  public void setMitMaxCallTime(java.lang.Integer value) {
    this.mit_max_call_time = value;
  }

  /** Creates a new MaxCallTime RecordBuilder */
  public static com.raiyi.model.MaxCallTime.Builder newBuilder() {
    return new com.raiyi.model.MaxCallTime.Builder();
  }
  
  /** Creates a new MaxCallTime RecordBuilder by copying an existing Builder */
  public static com.raiyi.model.MaxCallTime.Builder newBuilder(com.raiyi.model.MaxCallTime.Builder other) {
    return new com.raiyi.model.MaxCallTime.Builder(other);
  }
  
  /** Creates a new MaxCallTime RecordBuilder by copying an existing MaxCallTime instance */
  public static com.raiyi.model.MaxCallTime.Builder newBuilder(com.raiyi.model.MaxCallTime other) {
    return new com.raiyi.model.MaxCallTime.Builder(other);
  }
  
  /**
   * RecordBuilder for MaxCallTime instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<MaxCallTime>
    implements org.apache.avro.data.RecordBuilder<MaxCallTime> {

    private int tele_max_call_time;
    private int mit_max_call_time;

    /** Creates a new Builder */
    private Builder() {
      super(com.raiyi.model.MaxCallTime.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.raiyi.model.MaxCallTime.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing MaxCallTime instance */
    private Builder(com.raiyi.model.MaxCallTime other) {
            super(com.raiyi.model.MaxCallTime.SCHEMA$);
      if (isValidValue(fields()[0], other.tele_max_call_time)) {
        this.tele_max_call_time = data().deepCopy(fields()[0].schema(), other.tele_max_call_time);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.mit_max_call_time)) {
        this.mit_max_call_time = data().deepCopy(fields()[1].schema(), other.mit_max_call_time);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'tele_max_call_time' field */
    public java.lang.Integer getTeleMaxCallTime() {
      return tele_max_call_time;
    }
    
    /** Sets the value of the 'tele_max_call_time' field */
    public com.raiyi.model.MaxCallTime.Builder setTeleMaxCallTime(int value) {
      validate(fields()[0], value);
      this.tele_max_call_time = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'tele_max_call_time' field has been set */
    public boolean hasTeleMaxCallTime() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'tele_max_call_time' field */
    public com.raiyi.model.MaxCallTime.Builder clearTeleMaxCallTime() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'mit_max_call_time' field */
    public java.lang.Integer getMitMaxCallTime() {
      return mit_max_call_time;
    }
    
    /** Sets the value of the 'mit_max_call_time' field */
    public com.raiyi.model.MaxCallTime.Builder setMitMaxCallTime(int value) {
      validate(fields()[1], value);
      this.mit_max_call_time = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'mit_max_call_time' field has been set */
    public boolean hasMitMaxCallTime() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'mit_max_call_time' field */
    public com.raiyi.model.MaxCallTime.Builder clearMitMaxCallTime() {
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public MaxCallTime build() {
      try {
        MaxCallTime record = new MaxCallTime();
        record.tele_max_call_time = fieldSetFlags()[0] ? this.tele_max_call_time : (java.lang.Integer) defaultValue(fields()[0]);
        record.mit_max_call_time = fieldSetFlags()[1] ? this.mit_max_call_time : (java.lang.Integer) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}

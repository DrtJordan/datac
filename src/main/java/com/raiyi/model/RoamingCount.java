/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.raiyi.model;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class RoamingCount extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"RoamingCount\",\"namespace\":\"com.raiyi.model\",\"fields\":[{\"name\":\"not_roaming_count\",\"type\":\"int\",\"doc\":\"非漫游联网次数\",\"default\":-1},{\"name\":\"in_province_roaming_count\",\"type\":\"int\",\"doc\":\"省内漫游联网次数\",\"default\":-1},{\"name\":\"inter_province_roaming_count\",\"type\":\"int\",\"doc\":\"省际漫游联网次数\",\"default\":-1},{\"name\":\"international_roaming_count\",\"type\":\"int\",\"doc\":\"国际漫游联网次数\",\"default\":-1}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  /** 非漫游联网次数 */
  @Deprecated public int not_roaming_count;
  /** 省内漫游联网次数 */
  @Deprecated public int in_province_roaming_count;
  /** 省际漫游联网次数 */
  @Deprecated public int inter_province_roaming_count;
  /** 国际漫游联网次数 */
  @Deprecated public int international_roaming_count;

  /**
   * Default constructor.
   */
  public RoamingCount() {}

  /**
   * All-args constructor.
   */
  public RoamingCount(java.lang.Integer not_roaming_count, java.lang.Integer in_province_roaming_count, java.lang.Integer inter_province_roaming_count, java.lang.Integer international_roaming_count) {
    this.not_roaming_count = not_roaming_count;
    this.in_province_roaming_count = in_province_roaming_count;
    this.inter_province_roaming_count = inter_province_roaming_count;
    this.international_roaming_count = international_roaming_count;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return not_roaming_count;
    case 1: return in_province_roaming_count;
    case 2: return inter_province_roaming_count;
    case 3: return international_roaming_count;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: not_roaming_count = (java.lang.Integer)value$; break;
    case 1: in_province_roaming_count = (java.lang.Integer)value$; break;
    case 2: inter_province_roaming_count = (java.lang.Integer)value$; break;
    case 3: international_roaming_count = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'not_roaming_count' field.
   * 非漫游联网次数   */
  public java.lang.Integer getNotRoamingCount() {
    return not_roaming_count;
  }

  /**
   * Sets the value of the 'not_roaming_count' field.
   * 非漫游联网次数   * @param value the value to set.
   */
  public void setNotRoamingCount(java.lang.Integer value) {
    this.not_roaming_count = value;
  }

  /**
   * Gets the value of the 'in_province_roaming_count' field.
   * 省内漫游联网次数   */
  public java.lang.Integer getInProvinceRoamingCount() {
    return in_province_roaming_count;
  }

  /**
   * Sets the value of the 'in_province_roaming_count' field.
   * 省内漫游联网次数   * @param value the value to set.
   */
  public void setInProvinceRoamingCount(java.lang.Integer value) {
    this.in_province_roaming_count = value;
  }

  /**
   * Gets the value of the 'inter_province_roaming_count' field.
   * 省际漫游联网次数   */
  public java.lang.Integer getInterProvinceRoamingCount() {
    return inter_province_roaming_count;
  }

  /**
   * Sets the value of the 'inter_province_roaming_count' field.
   * 省际漫游联网次数   * @param value the value to set.
   */
  public void setInterProvinceRoamingCount(java.lang.Integer value) {
    this.inter_province_roaming_count = value;
  }

  /**
   * Gets the value of the 'international_roaming_count' field.
   * 国际漫游联网次数   */
  public java.lang.Integer getInternationalRoamingCount() {
    return international_roaming_count;
  }

  /**
   * Sets the value of the 'international_roaming_count' field.
   * 国际漫游联网次数   * @param value the value to set.
   */
  public void setInternationalRoamingCount(java.lang.Integer value) {
    this.international_roaming_count = value;
  }

  /** Creates a new RoamingCount RecordBuilder */
  public static com.raiyi.model.RoamingCount.Builder newBuilder() {
    return new com.raiyi.model.RoamingCount.Builder();
  }
  
  /** Creates a new RoamingCount RecordBuilder by copying an existing Builder */
  public static com.raiyi.model.RoamingCount.Builder newBuilder(com.raiyi.model.RoamingCount.Builder other) {
    return new com.raiyi.model.RoamingCount.Builder(other);
  }
  
  /** Creates a new RoamingCount RecordBuilder by copying an existing RoamingCount instance */
  public static com.raiyi.model.RoamingCount.Builder newBuilder(com.raiyi.model.RoamingCount other) {
    return new com.raiyi.model.RoamingCount.Builder(other);
  }
  
  /**
   * RecordBuilder for RoamingCount instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<RoamingCount>
    implements org.apache.avro.data.RecordBuilder<RoamingCount> {

    private int not_roaming_count;
    private int in_province_roaming_count;
    private int inter_province_roaming_count;
    private int international_roaming_count;

    /** Creates a new Builder */
    private Builder() {
      super(com.raiyi.model.RoamingCount.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.raiyi.model.RoamingCount.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing RoamingCount instance */
    private Builder(com.raiyi.model.RoamingCount other) {
            super(com.raiyi.model.RoamingCount.SCHEMA$);
      if (isValidValue(fields()[0], other.not_roaming_count)) {
        this.not_roaming_count = data().deepCopy(fields()[0].schema(), other.not_roaming_count);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.in_province_roaming_count)) {
        this.in_province_roaming_count = data().deepCopy(fields()[1].schema(), other.in_province_roaming_count);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.inter_province_roaming_count)) {
        this.inter_province_roaming_count = data().deepCopy(fields()[2].schema(), other.inter_province_roaming_count);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.international_roaming_count)) {
        this.international_roaming_count = data().deepCopy(fields()[3].schema(), other.international_roaming_count);
        fieldSetFlags()[3] = true;
      }
    }

    /** Gets the value of the 'not_roaming_count' field */
    public java.lang.Integer getNotRoamingCount() {
      return not_roaming_count;
    }
    
    /** Sets the value of the 'not_roaming_count' field */
    public com.raiyi.model.RoamingCount.Builder setNotRoamingCount(int value) {
      validate(fields()[0], value);
      this.not_roaming_count = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'not_roaming_count' field has been set */
    public boolean hasNotRoamingCount() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'not_roaming_count' field */
    public com.raiyi.model.RoamingCount.Builder clearNotRoamingCount() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'in_province_roaming_count' field */
    public java.lang.Integer getInProvinceRoamingCount() {
      return in_province_roaming_count;
    }
    
    /** Sets the value of the 'in_province_roaming_count' field */
    public com.raiyi.model.RoamingCount.Builder setInProvinceRoamingCount(int value) {
      validate(fields()[1], value);
      this.in_province_roaming_count = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'in_province_roaming_count' field has been set */
    public boolean hasInProvinceRoamingCount() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'in_province_roaming_count' field */
    public com.raiyi.model.RoamingCount.Builder clearInProvinceRoamingCount() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'inter_province_roaming_count' field */
    public java.lang.Integer getInterProvinceRoamingCount() {
      return inter_province_roaming_count;
    }
    
    /** Sets the value of the 'inter_province_roaming_count' field */
    public com.raiyi.model.RoamingCount.Builder setInterProvinceRoamingCount(int value) {
      validate(fields()[2], value);
      this.inter_province_roaming_count = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'inter_province_roaming_count' field has been set */
    public boolean hasInterProvinceRoamingCount() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'inter_province_roaming_count' field */
    public com.raiyi.model.RoamingCount.Builder clearInterProvinceRoamingCount() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'international_roaming_count' field */
    public java.lang.Integer getInternationalRoamingCount() {
      return international_roaming_count;
    }
    
    /** Sets the value of the 'international_roaming_count' field */
    public com.raiyi.model.RoamingCount.Builder setInternationalRoamingCount(int value) {
      validate(fields()[3], value);
      this.international_roaming_count = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'international_roaming_count' field has been set */
    public boolean hasInternationalRoamingCount() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'international_roaming_count' field */
    public com.raiyi.model.RoamingCount.Builder clearInternationalRoamingCount() {
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    public RoamingCount build() {
      try {
        RoamingCount record = new RoamingCount();
        record.not_roaming_count = fieldSetFlags()[0] ? this.not_roaming_count : (java.lang.Integer) defaultValue(fields()[0]);
        record.in_province_roaming_count = fieldSetFlags()[1] ? this.in_province_roaming_count : (java.lang.Integer) defaultValue(fields()[1]);
        record.inter_province_roaming_count = fieldSetFlags()[2] ? this.inter_province_roaming_count : (java.lang.Integer) defaultValue(fields()[2]);
        record.international_roaming_count = fieldSetFlags()[3] ? this.international_roaming_count : (java.lang.Integer) defaultValue(fields()[3]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}

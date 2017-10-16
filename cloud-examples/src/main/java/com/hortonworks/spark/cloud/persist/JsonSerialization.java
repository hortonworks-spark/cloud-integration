/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.cloud.persist;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Support for marshalling objects to and from JSON.
 *  <p>
 * It constructs an object mapper as an instance field.
 * and synchronizes access to those methods
 * which use the mapper.
 *
 * This method was copied from
 * {@code org.apache.hadoop.registry.client.binding.JsonSerDeser}.
 * @param <T> Type to marshal.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JsonSerialization<T> {

  private static final Logger LOG = LoggerFactory.getLogger(
      JsonSerialization.class);
  private static final String UTF_8 = "UTF-8";

  private final Class<T> classType;
  private final ObjectMapper mapper;

  /**
   * Create an instance bound to a specific type.
   * @param classType class to marshall
   * @param failOnUnknownProperties fail if an unknown property is encountered.
   * @param pretty generate pretty (indented) output?
   */
  public JsonSerialization(Class<T> classType,
      boolean failOnUnknownProperties, boolean pretty) {
    Preconditions.checkArgument(classType != null, "null classType");
    this.classType = classType;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
        failOnUnknownProperties);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, pretty);
  }

  /**
   * Get the simple name of the class type to be marshalled.
   * @return the name of the class being marshalled
   */
  public String getName() {
    return classType.getSimpleName();
  }

  /**
   * Convert from JSON.
   *
   * @param json input
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonParseException If the input is not well-formatted
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  @SuppressWarnings("unchecked")
  public synchronized T fromJson(String json)
      throws IOException, JsonParseException, JsonMappingException {
    if (json.isEmpty()) {
      throw new EOFException("No data");
    }
    try {
      return mapper.readValue(json, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json : {}\n{}", e, json, e);
      throw e;
    }
  }

  /**
   * Convert from a JSON file.
   * @param jsonFile input file
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonParseException If the input is not well-formatted
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  @SuppressWarnings("unchecked")
  public synchronized T fromFile(File jsonFile)
      throws IOException, JsonParseException, JsonMappingException {
    if (!jsonFile.isFile()) {
      throw new FileNotFoundException("Not a file: " + jsonFile);
    }
    if (jsonFile.length() == 0) {
      throw new EOFException("File is empty: " + jsonFile);
    }
    try {
      return mapper.readValue(jsonFile, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json file {}", jsonFile, e);
      throw e;
    }
  }

  /**
   * Convert from a JSON file.
   * @param resource input file
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonParseException If the input is not well-formatted
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  @SuppressWarnings({"IOResourceOpenedButNotSafelyClosed"})
  public synchronized T fromResource(String resource)
      throws IOException, JsonParseException, JsonMappingException {
    InputStream resStream = null;
    try {
      resStream = this.getClass().getResourceAsStream(resource);
      if (resStream == null) {
        throw new FileNotFoundException(resource);
      }
      return mapper.readValue(resStream, classType);
    } catch (IOException e) {
      LOG.error("Exception while parsing json resource {}", resource, e);
      throw e;
    } finally {
      IOUtils.closeStream(resStream);
    }
  }

  /**
   * clone by converting to JSON and back again.
   * This is much less efficient than any Java clone process.
   * @param instance instance to duplicate
   * @return a new instance
   * @throws IOException IO problems.
   */
  public T fromInstance(T instance) throws IOException {
    return fromJson(toJson(instance));
  }

  /**
   * Load from a Hadoop filesystem.
   * @param fs filesystem
   * @param path path
   * @return a loaded CD
   * @throws IOException IO problems
   * @throws EOFException if not enough bytes were read in
   * @throws JsonParseException parse problems
   * @throws JsonMappingException O/J mapping problems
   */
  public T load(FileSystem fs, Path path)
      throws IOException, JsonParseException, JsonMappingException {
    return load(fs, path, true);
  }

  /**
   * Load from a Hadoop filesystem, optionally skip verifying the length of
   * the data exactly matches the expected length. That can work around an
   * issue where object store encryption (specifically: S3 client side
   * encryption) returns less bytes on a stream read than the declared length.
   * @param fs filesystem
   * @param path path
   * @return a loaded CD
   * @throws IOException IO problems
   * @throws EOFException if not enough bytes were read in
   * @throws JsonParseException parse problems
   * @throws JsonMappingException O/J mapping problems
   */
  public T load(FileSystem fs, Path path, boolean verifyLength)
      throws IOException, JsonParseException, JsonMappingException {
    FileStatus status = fs.getFileStatus(path);
    if (status.isDirectory()) {
      throw new FileNotFoundException("Not a file: " + path);
    }
    long len = status.getLen();
    if (len == 0) {
      throw new EOFException("File is empty: " + path);
    }
    byte[] b = new byte[(int) len];
    FSDataInputStream dataInputStream = fs.open(path);
    dataInputStream.readFully(0, b);
    return fromBytes(path.toString(), b);
  }

  /**
   * Save to a Hadoop filesystem.
   * @param fs filesystem
   * @param path path
   * @param overwrite should any existing file be overwritten
   * @throws IOException IO exception
   */
  public void save(FileSystem fs, Path path, T instance,
      boolean overwrite) throws
      IOException {
    writeJsonAsBytes(instance, fs.create(path, overwrite));
  }

  /**
   * Write the JSON as bytes, then close the file.
   * @param dataOutputStream an output stream that will always be closed
   * @throws IOException on any failure
   */
  private void writeJsonAsBytes(T instance,
      DataOutputStream dataOutputStream) throws IOException {
    try {
      dataOutputStream.write(toBytes(instance));
    } finally {
      dataOutputStream.close();
    }
  }

  /**
   * Convert JSON to bytes.
   * @param instance instance to convert
   * @return a byte array
   * @throws IOException IO problems
   */
  public byte[] toBytes(T instance) throws IOException {
    return mapper.writeValueAsBytes(instance);
  }

  /**
   * Deserialize from a byte array.
   * @param path path the data came from
   * @param bytes byte array
   * @throws IOException IO problems
   * @throws EOFException not enough data
   */
  public T fromBytes(String path, byte[] bytes) throws IOException {
    return fromJson(new String(bytes, 0, bytes.length, UTF_8));
  }

  /**
   * Convert an instance to a JSON string.
   * @param instance instance to convert
   * @return a JSON string description
   * @throws JsonProcessingException Json generation problems
   */
  public synchronized String toJson(T instance) throws JsonProcessingException {
    return mapper.writeValueAsString(instance);
  }

  /**
   * Convert an instance to a string form for output. This is a robust
   * operation which will convert any JSON-generating exceptions into
   * error text.
   * @param instance non-null instance
   * @return a JSON string
   */
  public String toString(T instance) {
    Preconditions.checkArgument(instance != null, "Null instance argument");
    try {
      return toJson(instance);
    } catch (JsonProcessingException e) {
      return "Failed to convert to a string: " + e;
    }
  }
}

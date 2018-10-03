/*-
 * -\-\-
 * Spotify Styx API Client
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.styx.client;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ryanharter.auto.value.gson.GenerateTypeAdapter;
import io.norberg.automatter.gson.AutoMatterTypeAdapterFactory;
import net.dongliu.gson.GsonJava8TypeAdapterFactory;

class Json {

  private Json() {
    throw new UnsupportedOperationException();
  }

  static final Gson GSON = new GsonBuilder()
      .registerTypeAdapterFactory(new AutoMatterTypeAdapterFactory())
      .registerTypeAdapterFactory(GenerateTypeAdapter.FACTORY)
      .registerTypeAdapterFactory(new GsonJava8TypeAdapterFactory())
      .setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES)
      .create();
}

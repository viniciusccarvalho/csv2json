/*
 * Copyright 2017 the original author or authors.
 *
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
 */

package org.springframework.cloud.stream.app.csv2json.processor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.core.io.UrlResource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.ObjectUtils;

/**
 * @author Vinicius Carvalho
 */
public class CSV2JsonProcessor implements InitializingBean {

	private Csv2JsonProcessorProperties properties;

	private Source source;

	private Map<String, String> aliases = new HashMap<>();

	public CSV2JsonProcessor(Csv2JsonProcessorProperties properties, Source source) {
		this.properties = properties;
		this.source = source;
	}

	@StreamListener(Processor.INPUT)
	public void receive(Message message) {
		String url = message.getPayload().toString();
		try {
			UrlResource resource = new UrlResource(url);
			CSVFormat format = resolveFormat();
			if(format == null){
				throw new IllegalArgumentException("Can not parse file, invalid format provided");
			}
			format = format.withDelimiter(properties.getDelimiter()).withFirstRecordAsHeader();
			Iterable<CSVRecord> records = CSVParser.parse(resource.getURL(), Charset.defaultCharset(), format);
			for (CSVRecord record : records) {
				Map<String, String> payload = filter(record.toMap());
				source.output().send(MessageBuilder.withPayload(payload)
						.setHeader(MessageHeaders.CONTENT_TYPE, properties.getContentType()).build());
			}
		}
		catch (MalformedURLException e) {
			throw new IllegalArgumentException("Invalid URL can't parse contents");
		}
		catch (IOException e) {
			throw new IllegalStateException("Can't open resource");
		}
	}

	private CSVFormat resolveFormat(){
		CSVFormat format = null;
		for(CSVFormat.Predefined value  : CSVFormat.Predefined.values()){
			if(value.name().equalsIgnoreCase(properties.getFormat())){
				format = CSVFormat.valueOf(value.name());
				break;
			}
		}
		return format;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if(this.properties.getAliases() != null){
			for (String alias : this.properties.getAliases()) {
				String[] values = alias.split(":");
				aliases.put(values[0], values[1]);
			}
		}
	}

	private Map<String, String> filter(Map<String, String> original) {
		Map<String,String> transformed = new HashMap<>();
		for(Map.Entry<String,String> entry : original.entrySet()){
			if(includes(entry.getKey())){
				String key = aliases.get(entry.getKey()) != null ? aliases.get(entry.getKey()) : entry.getKey();
				transformed.put(key,entry.getValue());
			}
		}
		return transformed;
	}

	private boolean includes(String key){
		if(ObjectUtils.isEmpty(this.properties.getIncludes()) || this.properties.getIncludes().contains(key)){
			return (ObjectUtils.isEmpty(this.properties.getExcludes()) || !this.properties.getExcludes().contains(key));
		}
		return false;
	}

}

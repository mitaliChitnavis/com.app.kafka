package com.ibm.app.enums;

import java.util.Iterator;

public enum CountryEnum {
UnitedKingdom("United Kingdom","topicUK"),
France("France","topicFrance"),
Australia("Australia","topicAust"),
Netherlands("Netherlands","topicNether"),
Germany("Germany","topicGermany");


public String country;
public String topicName;
public String getCountry() {
	return country;
}
public String getTopicName() {
	return topicName;
}

private CountryEnum(String country, String topicName) {
	this.country = country;
	this.topicName = topicName;
}

public static CountryEnum getCountry(String country)
{
	for(CountryEnum e : values())
	{
	if(e.country.equalsIgnoreCase(country))
	{
		return e;
	}
	}
	return null;
	
}
}

package ru.itmo.hadoop;

public class Metrics {
	public enum Metric {
		SUM,
		AVERAGE,
		COUNT;
	}

	public static boolean isValid(String metric) {
		for (Metric m : Metric.values()) {
			if (m.name().equalsIgnoreCase(metric)) {
				return true;
			}
		}
		return false;
	}
}

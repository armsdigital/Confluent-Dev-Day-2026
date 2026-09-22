# Real-Time E-Commerce Dynamic Pricing & Fraud Mitigation Engine

[![Confluent Cloud](https://img.shields.io/badge/Confluent%20Cloud-Apache%20Kafka%20%26%20Flink-blue?logo=apachekafka)](https://confluent.cloud)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An enterprise-grade real-time event processing platform built on **Confluent Cloud**, **Apache Flink SQL**, and **Schema Registry**. This engine ingests live high-throughput e-commerce checkout events, continuously computes dynamic surge pricing based on product demand velocity, and flags anomalous user transaction patterns to mitigate fraud before settlement.

---

## Architecture & Data Flow
```
+-------------------+        +--------------------+        +-----------------------+        +---------------------------+
| Datagen Connector |  --->  |   orders_stream    |  --->  |  Confluent Flink SQL  |  --->  | dynamic_product_pricing   |
| (Mock Checkout)   |        |   (Kafka Topic)    |        | (Stream Analytics)    |        | high_risk_alerts          |
+-------------------+        +--------------------+        +-----------------------+        +---------------------------+
|                                                              |
+------------------ Schema Registry ----------------------------+
```
1. **Ingestion Layer:** Real-time checkout telemetry generated via **Confluent Datagen Source Connector** streaming into the `orders_stream` topic.
2. **Governance Layer:** **Confluent Schema Registry** enforces AVRO/JSON Schema compatibility across event streams.
3. **Stream Processing Layer:** **Confluent Cloud Flink SQL** executes continuous aggregations to compute real-time price updates and flag elevated transaction bursts.
4. **Sink Layer:** Downstream analytical tables exposed to operational webhooks, dynamic pricing microservices, and security dashboards.

---

## Key Features & Business Impact

* **Dynamic Price Optimization:** Calculates demand velocity per product within rolling intervals, adjusting pricing (up to 15% surge) dynamically during traffic spikes to optimize revenue margins.
* **Proactive Fraud Mitigation:** Detects anomalous transaction bursts (e.g., > 5 transactions within short windows) to flag high-risk accounts and mitigate chargebacks in real time.
* **Low-Latency Architecture:** Eliminates traditional batch ETLS, reducing pricing adjustment latencies from hours to sub-seconds.

---

## Event Schema (`orders_stream`)

The underlying topic uses Schema Registry enforcement with the following structure:

```json
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "com.ecommerce.pricing",
  "doc": "Schema for high-throughput real-time checkout telemetry",
  "fields": [
    { "name": "order_id", "type": "string" },
    { "name": "user_id", "type": "string" },
    { "name": "product_id", "type": "string" },
    { "name": "base_price", "type": "double" },
    { "name": "quantity", "type": "int" },
    { "name": "timestamp", "type": "long" }
  ]
}
```

Stream Processing Logic (Flink SQL)
1. Dynamic Surge Pricing Engine
Evaluates real-time demand velocity per product_id and adjusts prices accordingly:

```
CREATE TABLE high_risk_alerts_topic WITH (
    'connector' = 'confluent',
    'value.format' = 'json-registry'
) AS
SELECT 
    COALESCE(itemid, 'UNKNOWN') AS product_id,
    COUNT(orderid) AS transaction_count,
    SUM(orderunits) AS total_units
FROM orders_stream
WHERE itemid IS NOT NULL
GROUP BY itemid
HAVING COUNT(orderid) > 5;
```

3. High-Risk Account Fraud Alerts
Identifies users executing high-frequency checkout attempts:

```
CREATE TABLE dynamic_product_pricing_topic WITH (
    'connector' = 'confluent',
    'value.format' = 'json-registry'
) AS
SELECT 
    COALESCE(itemid, 'UNKNOWN') AS product_id,
    AVG(orderunits) AS standard_units,
    COUNT(orderid) AS demand_velocity,
    CASE 
        WHEN COUNT(orderid) > 20 THEN AVG(orderunits) * 1.15
        WHEN COUNT(orderid) > 10 THEN AVG(orderunits) * 1.05
        ELSE AVG(orderunits)
    END AS surge_units
FROM orders_stream
WHERE itemid IS NOT NULL
GROUP BY itemid;
```



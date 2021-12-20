# Pandaproxy API
## Version: 1.0.0

### /consumers/{group_name}

#### POST
##### Summary

Create a consumer for the group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| group_name | path |  | Yes | string |
| properties | body |  | No | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | object |
| 409 | Consumer already exists | [error_body](#error_body) |
| 422 | Invalid configuration | [error_body](#error_body) |

### /consumers/{group_name}/instances/{instance}

#### DELETE
##### Summary

Remove a consumer for the group

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| group_name | path |  | Yes | string |
| instance | path |  | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 204 |  |  |
| 404 | Consumer not found | [error_body](#error_body) |

### /consumers/{group_name}/instances/{instance}/offsets

#### GET
##### Summary

Get committed group offsets for given partitions

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| group_name | path |  | Yes | string |
| instance | path |  | Yes | string |
| offsets | body |  | No | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 |  | object |
| 404 | Consumer, topic, or partition not found | [error_body](#error_body) |

#### POST
##### Summary

Commit offsets for a consumer

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| group_name | path |  | Yes | string |
| instance | path |  | Yes | string |
| offsets | body |  | No | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 204 |  |  |
| 404 | Consumer, topic, or partition not found | [error_body](#error_body) |

### /consumers/{group_name}/instances/{instance}/records

#### GET
##### Summary

Fetch data for the consumer assignments

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| group_name | path |  | Yes | string |
| instance | path |  | Yes | string |
| timeout | query |  | No | integer |
| max_bytes | query |  | No | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 |  | object |
| 404 | Consumer, topic, or partition not found | [error_body](#error_body) |

### /consumers/{group_name}/instances/{instance}/subscription

#### POST
##### Summary

Subscribe a consumer group to topics

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| group_name | path |  | Yes | string |
| instance | path |  | Yes | string |
| topics | body |  | No | [ string ] |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 204 |  |  |
| 404 | Consumer, topic, or partition not found | [error_body](#error_body) |

### /topics

#### GET
##### Summary

Get a list of Kafka topics.

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | Array of topic names | [ string ] |

### /topics/{topic_name}

#### POST
##### Summary

Produce messages to a topic.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| topic_name | path |  | Yes | string |
| records | body |  | No | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 |  | object |

### /topics/{topic_name}/partitions/{partition_id}/records

#### GET
##### Summary

Get records from a topic.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| topic_name | path |  | Yes | string |
| partition_id | path |  | Yes | integer |
| offset | query |  | Yes | integer |
| timeout | query |  | Yes | integer |
| max_bytes | query |  | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 |  | object |
| 404 | Topic or partition not found | [error_body](#error_body) |

### Models

#### error_body

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| error_code | integer |  | No |
| message | string |  | No |

# Pandaproxy Schema Registry
## Version: v21.12.1

### /config/{subject}

#### GET
##### Summary

Get the compatibility level for a subject.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | object |
| 500 | Internal Server error | [error_body](#error_body) |

#### PUT
##### Summary

Set the compatibility level for a subject.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| config | body |  | No | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | object |
| 500 | Internal Server error | [error_body](#error_body) |

### /config

#### GET
##### Summary

Get the global compatibility level.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | object |
| 500 | Internal Server error | [error_body](#error_body) |

#### PUT
##### Summary

Set the global compatibility level.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| config | body |  | No | object |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | object |
| 500 | Internal Server error | [error_body](#error_body) |

### /schemas/types

#### GET
##### Summary

Get the supported schema types.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ string ] |
| 500 | Internal Server error | [error_body](#error_body) |

### /schemas/ids/{id}

#### GET
##### Summary

Get a schema by id.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path |  | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | object |
| 404 | Schema not found | [error_body](#error_body) |
| 500 | Internal Server error | [error_body](#error_body) |

### /schemas/ids/{id}/versions

#### GET
##### Summary

Get a list of subject-version for the schema id.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path |  | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ object ] |
| 404 | Schema not found | [error_body](#error_body) |
| 500 | Internal Server error | [error_body](#error_body) |

### /subjects

#### GET
##### Summary

Retrieve a list of subjects.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| deleted | query |  | No | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ string ] |
| 500 | Internal Server Error | [error_body](#error_body) |

### /subjects/{subject}

#### POST
##### Summary

Check if a schema is already registred for the subject.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| schema_def | body |  | No | [schema_def](#schema_def) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [subject_schema](#subject_schema) |
| 409 | Incompatible schema | [error_body](#error_body) |
| 422 | Invalid schema | [error_body](#error_body) |
| 500 | Internal Server Error | [error_body](#error_body) |

#### DELETE
##### Summary

Delete all schemas for the subject.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| permanent | query |  | No | boolean |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ integer ] |
| 404 | Subject not found | [error_body](#error_body) |
| 500 | Internal Server Error | [error_body](#error_body) |

### /subjects/{subject}/versions

#### GET
##### Summary

Retrieve a list of versions for a subject.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| deleted | query |  | No | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ integer ] |
| 404 | Subject not found | [error_body](#error_body) |
| 500 | Internal Server Error | [error_body](#error_body) |

#### POST
##### Summary

Create a new schema for the subject.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| schema_def | body |  | No | [schema_def](#schema_def) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | object |
| 409 | Incompatible schema | [error_body](#error_body) |
| 422 | Invalid schema | [error_body](#error_body) |
| 500 | Internal Server Error | [error_body](#error_body) |

### /subjects/{subject}/versions/{version}

#### GET
##### Summary

Retrieve a schema for the subject and version.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| version | path |  | Yes | string |
| deleted | query |  | No | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [subject_schema](#subject_schema) |
| 404 | Schema not found | [error_body](#error_body) |
| 422 | Invalid version | [error_body](#error_body) |
| 500 | Internal Server Error | [error_body](#error_body) |

#### DELETE
##### Summary

Delete a schema for the subject and version.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| version | path |  | Yes | string |
| permanent | query |  | No | boolean |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | integer |
| 404 | Schema not found | [error_body](#error_body) |
| 422 | Invalid version | [error_body](#error_body) |
| 500 | Internal Server Error | [error_body](#error_body) |

### /subjects/{subject}/versions/{version}/schema

#### GET
##### Summary

Retrieve a schema for the subject and version.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| version | path |  | Yes | string |
| deleted | query |  | No | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | string |
| 404 | Schema not found | [error_body](#error_body) |
| 422 | Invalid version | [error_body](#error_body) |
| 500 | Internal Server Error | [error_body](#error_body) |

### /subjects/{subject}/versions/{version}/referencedBy

#### GET
##### Summary

Retrieve a list of schema ids that reference the subject and version.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| version | path |  | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ integer ] |
| 404 | Schema not found | [error_body](#error_body) |
| 422 | Invalid version | [error_body](#error_body) |
| 500 | Internal Server Error | [error_body](#error_body) |

### /compatibility/subjects/{subject}/versions/{version}

#### POST
##### Summary

Test compatibility of a schema for the subject and version.

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| subject | path |  | Yes | string |
| version | path |  | Yes | integer |
| schema_def | body |  | No | [schema_def](#schema_def) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | object |
| 409 | Incompatible schema | [error_body](#error_body) |
| 422 | Invalid schema | [error_body](#error_body) |
| 500 | Internal Server Error | [error_body](#error_body) |

### Models

#### error_body

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| error_code | integer |  | No |
| message | string |  | No |

#### schema_def

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| schema | string |  | No |
| schemaType | string |  | No |
| references | [ object ] |  | No |

#### subject_schema

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| subject | string |  | No |
| version | integer |  | No |
| id | integer |  | No |
| schemaType | string |  | No |
| references | [ object ] |  | No |
| schema | string |  | No |

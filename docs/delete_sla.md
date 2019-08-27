# create_sla

Delete an SLA from the Rubrik cluster

```py
def delete_sla(self, name, timeout=15)
```

## Arguments

| Name | Type | Description                    | Choices |
|------|------|--------------------------------|---------|
| name | str  | The name of the SLA to delete. |         |

## Keyword Arguments

| Name    | Type | Description                                                                                                  | Choices | Default |
|---------|------|--------------------------------------------------------------------------------------------------------------|---------|---------|
| timeout | str  | The number of seconds to wait to establish a connection the Rubrik cluster before returning a timeout error. |         | 15      |

## Returns

| Type | Return Value                                                              |
|------|---------------------------------------------------------------------------|
| str  | No change required. The SLA Domain '`name`' is not on the Rubrik cluster. |
| dict | The full API response for `DELETE  /v1/sla_domain/{id}`.                  |
| dict | The full API response for `DELETE  /v2/sla_domain/{id}`.                  |

## Example


```py
import rubrik_cdm

rubrik = rubrik_cdm.Connect()

sla_name = "PythonSDK"

delete_sla = rubrik.delete_sla(sla_name)
print(delete_sla)
```


# hass-mcp

strives to be full-featured mcp server for home assistant. no feature is not worth implementing. if it's missing, it's a bug. pulls accepted.

## use cases

things like this should be possible:

> user: label all of my devices using an optimal device taxonomy
> _(3 hours later)_ assistant: i've implemented the world's most efficient and optimized device taxonomy in the history of the universe!
> user: that's awesome actually

## install

```sh
cargo install hass-mcp
```

## usage

1. figure out your url, mine is http://10.10.10.55:8143
2. get a long lived access token from `/profile/security` page

```sh
hass-mcp serve <home-assistant-url> --api-key <long-lived-access-token>
```

## tools

- `add_labels_to_entity`
- `analyze_configuration`
- `assign_device_to_area`
- `assign_entity_to_area`
- `clear_entity_labels`
- `create_and_assign_label`
- `create_and_label_device`
- `create_area`
- `create_floor`
- `create_label`
- `delete_area`
- `delete_floor`
- `delete_label`
- `describe_config`
- `discover_tools`
- `find_area_by_name`
- `find_entities_by_label`
- `find_floor_by_name`
- `get_device_summary`
- `get_entities_for_device`
- `get_entities_in_area`
- `get_orphaned_entities`
- `get_summary`
- `get_unassigned_entities`
- `label_device`
- `list_all_resources`
- `list_areas`
- `list_devices`
- `list_entities`
- `list_floors`
- `list_labels`
- `list_sensors`
- `recommend_labeling`
- `search_entities`
- `update_area`
- `update_device`
- `update_entity`
- `validate_device_id`
- `validate_entity_id`

## license

mit or apache-2.0

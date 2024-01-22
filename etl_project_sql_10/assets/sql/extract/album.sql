{% set config = {
    "extract_type": "incremental",
    "incremental_column": "load_date",
    "source_table_name": "albums"
} %}

select
    id,
    album_id,
    album_name,
    album_type,
    artist_id,
    release_date,
    total_tracks,
    load_date
from
    {{ config["source_table_name"] }}

{% if is_incremental and incremental_value is not none %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}

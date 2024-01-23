{% set config = {
    "extract_type": "incremental",
    "incremental_column": "load_date",
    "source_table_name": "tracks"
} %}

select
    id,
    track_id,
    name,
    artist_id,
    album_id,
    duration_ms,
    popularity,
    explicit,
    preview_url,
    track_number,
    market,
    load_date

from
    {{ config["source_table_name"] }}

{% if is_incremental and incremental_value is not none %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}




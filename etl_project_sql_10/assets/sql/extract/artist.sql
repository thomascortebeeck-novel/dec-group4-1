{% set config = {
    "extract_type": "incremental",
    "incremental_column": "load_date",
    "source_table_name": "artists"
} %}

select
    artist_id,
    name,
    popularity,
    genres,
    followers,
    spotify_url,
    load_date
from
    {{ config["source_table_name"] }}

{% if is_incremental %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}
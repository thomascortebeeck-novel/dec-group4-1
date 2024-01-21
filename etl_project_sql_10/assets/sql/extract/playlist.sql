{% set config = {
    "extract_type": "full",
    "source_table_name": "playlist"
} %}

select
    artist_id,
    playlist_country
from
    {{ config["source_table_name"] }}

# `content_metadata_wide` workload

This workload models a single very wide content metadata table and biases traffic toward the observed hot update patterns:

- `page_image_data` + `metadata_revision` + `image_probe_result` + `raw_image_data` + `variant_image_data`
- `fetch_status_code` + `source_modified_time`
- `page_image_data` + `metadata_revision` + `raw_image_data` + `variant_image_data`

The first three update forms account for roughly 96% of generated updates. The remaining traffic uses the two lower-frequency SQL shapes from the sample.

`-row-size` controls the approximate width of the large mutable columns. Larger values mainly increase:

- `page_image_data`
- `raw_image_data`
- `image_probe_result`
- `body_data`
- `headline_language_vectors`

The generated rows are dominated by mutable blob and varbinary columns, so this workload is useful for stressing update-heavy write paths, row encoding, and replication of wide-row changes.

Use `-percentage-for-update` aggressively with this workload if the goal is to maximize update pressure.

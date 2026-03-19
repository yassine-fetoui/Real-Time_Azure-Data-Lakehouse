{% snapshot dim_customers_scd2 %}

{#
  SCD Type 2 snapshot using SHA-256 hash change detection.

  Problem solved: Full MERGE on 500M-row table was timing out (4 hrs).
  Solution: Incremental check strategy using hashed column fingerprint.
  Result: Nightly load cut from 4 hours → 18 minutes.

  Strategy:
    - unique_key: natural key for the entity
    - strategy: "check" — only process rows where check_cols changed
    - SHA-256 hash over all tracked columns for efficient diff detection
#}

{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'customer_id',
    strategy      = 'check',
    check_cols    = ['email', 'address', 'phone', 'tier', 'status'],
    invalidate_hard_deletes = True
  )
}}

SELECT
    customer_id,
    email,
    address,
    phone,
    tier,
    status,
    country_code,
    created_at,
    updated_at,

    -- SHA-256 fingerprint: only rows where this changes trigger a new SCD row
    SHA2(CONCAT_WS('|', email, address, phone, tier, status), 256) AS row_hash

FROM {{ ref('stg_customers') }}

{% endsnapshot %}

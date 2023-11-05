{{
    config(
        materialized='table',
        post_hook=["
        GRANT USAGE ON SCHEMA {{target.schema}} TO GROUP biusers;
        GRANT SELECT on TABLE {{target.schema}}.bireport TO GROUP biusers;
        
        "
        ]
    )
}}

select * from {{ref('joins')}}
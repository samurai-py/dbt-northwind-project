with calc_employees as (
    select
        *,
        cast(date_part(year, current_date) - date_part(year, birth_date) as int) as age,
        cast(date_part(year, current_date) - date_part(year, hire_date) as int) as lengthOfService,
        first_name || ' ' || last_name as name
    from {{source('sources', 'employees')}}
)
select * from calc_employees
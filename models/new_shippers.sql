select
    sh.*,
    se.shipper_email
from {{source('sources', 'shippers')}} as sh
left join {{ref('shippers_email')}} as se on (sh.shipper_id = se.shipper_id)
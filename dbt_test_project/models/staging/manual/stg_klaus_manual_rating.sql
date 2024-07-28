SELECT
    payment_id,
    team_id,
    review_id,
    category_id,
    rating,
    cause,
    rating_max,
    weight,
    critical AS is_critical,
    category_name,
FROM {{ source('klaus', 'manual_rating') }}
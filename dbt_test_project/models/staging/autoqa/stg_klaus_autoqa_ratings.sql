SELECT
    autoqa_rating_id,
    autoqa_review_id,
    payment_id,
    payment_token_id,
    external_ticket_id,
    team_id,
    reviewee_internal_id,
    rating_category_id,
    rating_category_name,
    score
FROM {{ source('klaus', 'autoqa_ratings') }}
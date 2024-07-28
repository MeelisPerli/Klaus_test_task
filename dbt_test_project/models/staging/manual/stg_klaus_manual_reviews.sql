SELECT
    review_id,
    payment_id,
    payment_token_id,
    created AS created_as,
    --conversion_created_at, Did not import this
    conversation_created_date,
    conversation_external_id,
    team_id,
    reviewer_id,
    reviewee_id,
    comment_id,
    scorecard_id,
    scorecard_tag,
    score,
    updated_at,
    updated_by,
    assignment_name,
    seen AS is_seen,
    disputed AS is_disputed,
    review_time_seconds,
    assignment_review,
    imported_at
FROM {{ source('klaus', 'manual_reviews') }}
